from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory

from sseclient import SSEClient as EventSource

from threading import Thread

import datetime
import json
import pytz
import time
import uuid


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)
        self.session.row_factory = named_tuple_factory

    def execute(self, query):
        return self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def write_data(self, time_created, domain, user_id, user_name, is_bot, page_title, page_url, page_id):
        query1 = "INSERT INTO page_creation (uid, time_created, domain, user_id, user_name, is_bot, page_title)" \
                 " VALUES (%s, '%s', '%s', '%s', '%s', %s, '%s')" % (uuid.uuid1(), time_created, domain, user_id,
                                                                     user_name, is_bot, page_title)
        query2 = "INSERT INTO pages (page_id, page_url) VALUES ('%s', '%s')" % (page_id, page_url)
        query3 = "INSERT INTO users_pages (uid, user_id, page_url) VALUES (%s, '%s', '%s')" % (uuid.uuid1(), user_id,
                                                                                               page_url)
        self.execute(query1)
        self.execute(query2)
        self.execute(query3)

    def write_statistics(self):
        time_now = datetime.datetime.now(pytz.utc)
        long_start_time = time_now.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=6)
        start_time = time_now.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
        end_time = start_time + datetime.timedelta(hours=1)

        query1 = "SELECT domain FROM page_creation WHERE time_created >= '%s' AND time_created < '%s' " \
                 "ALLOW FILTERING" %\
                 (start_time.strftime("%Y-%m-%d %H:%M:%S+0000"), end_time.strftime("%Y-%m-%d %H:%M:%S+0000"))
        rows = self.execute(query1)
        domains = {}
        for row in rows:
            if str(row.domain) in domains.keys():
                domains[str(row.domain)] += 1
            else:
                domains[str(row.domain)] = 1
        for domain in domains:
            all_pages = domains[domain]
            query2 = "INSERT INTO domain_statistics (uid, time_start, domain, all_pages) VALUES" \
                     " (%s, '%s', '%s', %s)" % (uuid.uuid1(), start_time.strftime("%Y-%m-%d %H:%M:%S+0000"),
                                                domain, all_pages,)
            self.execute(query2)

        query3 = "SELECT domain, is_bot FROM page_creation WHERE time_created >= '%s' AND time_created < '%s' " \
                 "ALLOW FILTERING" % \
                 (long_start_time.strftime("%Y-%m-%d %H:%M:%S+0000"), start_time.strftime("%Y-%m-%d %H:%M:%S+0000"))
        rows = self.execute(query3)
        domains = {}
        for row in rows:
            if not str(row.domain) in domains.keys():
                domains[str(row.domain)] = 0
            if row.is_bot:
                domains[str(row.domain)] += 1
        for domain in domains:
            query4 = "INSERT INTO bots_statistics (uid, time_start, domain, created_by_bot) VALUES" \
                     " (%s, '%s', '%s', %s)" % (uuid.uuid1(), long_start_time.strftime("%Y-%m-%d %H:%M:%S+0000"),
                                                domain, domains[domain])
            self.execute(query4)

        query5 = "SELECT user_id, user_name, page_title FROM page_creation WHERE time_created >= '%s' " \
                 "AND time_created < '%s' ALLOW FILTERING" % \
                 (long_start_time.strftime("%Y-%m-%d %H:%M:%S+0000"), start_time.strftime("%Y-%m-%d %H:%M:%S+0000"))
        rows = self.execute(query5)
        users = {}
        for row in rows:
            if not str(row.user_id) in users.keys():
                name = str(row.user_name)
                name = name.replace("'", "''")
                users[str(row.user_id)] = {
                    "user_name": name,
                    "pages_list": []
                }
            page_title = str(row.page_title)
            users[str(row.user_id)]["pages_list"].append(page_title)
        users = {k: v for k, v in sorted(users.items(), key=lambda item: len(item[1]["pages_list"]), reverse=True)[:20]}
        for user in users:
            titles = users[user]["pages_list"]
            pages_list = "["
            for title in titles:
                page_name = title.replace("'", "''")
                pages_list = pages_list + "'" + page_name + "'" + ", "
            pages_list = pages_list[:-2]
            pages_list += "]"
            query6 = "INSERT INTO users_statistics (uid, time_start, user_id, user_name, page_count, pages_list)" \
                     " VALUES (%s, '%s', '%s', '%s', %s, %s)" % (uuid.uuid1(),
                                                                 long_start_time.strftime("%Y-%m-%d %H:%M:%S+0000"),
                                                                 user, users[user]["user_name"],
                                                                 len(users[user]["pages_list"]),
                                                                 pages_list)
            self.execute(query6)


class WikimediaStreamReader:
    def __init__(self, cassandra_host, cassandra_port, keyspace):
        self.url = 'https://stream.wikimedia.org/v2/stream/page-create'
        self.event_source = EventSource(self.url)

        self.cassandra_client = CassandraClient(cassandra_host, cassandra_port, keyspace)
        self.cassandra_client.connect()

        self.statistic_client = CassandraClient(cassandra_host, cassandra_port, keyspace)
        self.statistic_client.connect()

        self.start = datetime.datetime.now(pytz.utc).replace(minute=0, second=0, microsecond=0)
        self.shutdown = False
        self.statistics = Thread(target=self.process_statistics)
        self.statistics.daemon = True
        self.statistics.start()

    def __del__(self):
        self.shutdown = True
        self.cassandra_client.close()
        self.statistic_client.close()

    def process_data(self):
        for event in self.event_source:
            if event.event == 'message':
                try:
                    page_data = json.loads(event.data)
                except ValueError:
                    pass
                else:
                    try:
                        meta = page_data["meta"]
                        performer = page_data["performer"]

                        domain = meta["domain"]
                        time_create = meta["dt"]
                        page_url = meta["uri"]
                        page_id = page_data["page_id"]
                        page_title = page_data["page_title"]
                        user_id = performer["user_id"]
                        user_name = performer["user_text"]
                        is_bot = performer["user_is_bot"]

                        page_title = page_title.replace("'", "''")
                        user_name = user_name.replace("'", "''")
                    except KeyError:
                        pass
                    else:
                        self.cassandra_client.write_data(time_create, domain, user_id, user_name, is_bot, page_title,
                                                         page_url, page_id)

    def process_statistics(self):
        while not self.shutdown:
            time_now = datetime.datetime.now(pytz.utc)
            if time_now - datetime.timedelta(hours=1) > self.start:
                self.statistic_client.write_statistics()
                self.start = time_now.replace(minute=0, second=0, microsecond=0)
            awake_time = time_now + datetime.timedelta(hours=1)
            awake_time = awake_time.replace(minute=1, second=0, microsecond=0)
            time.sleep((awake_time - time_now).total_seconds())


def main():
    cassandra_host = 'localhost'
    cassandra_port = 9042
    keyspace = 'wikipedia_dulher'

    reader = WikimediaStreamReader(cassandra_host, cassandra_port, keyspace)
    reader.process_data()


if __name__ == "__main__":
    main()
