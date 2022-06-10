import datetime
import pytz

from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory
from flask import jsonify, request, Flask


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

    def close(self):
        self.session.shutdown()

    def query1(self, time_start):
        """
        type A № 1
        Return the aggregated statistics containing the number of created pages foreach Wikipedia domain for
        each hour in the last 6 hours, excluding the last hour.
        :param time_start: time in format hh:mm:ss
        :return: Rows
        """
        query = "SELECT domain, all_pages FROM domain_statistics WHERE time_start='%s'" % time_start
        rows = self.session.execute(query)
        return rows

    def query2(self, time_start):
        """
        type A № 2
        Return the statistics about the number of pages created by bots for each of the domains for the last 6 hours,
        excluding the last hour.
        :param time_start: time in format hh:mm:ss
        :return: Rows
        """
        query = "SELECT domain, created_by_bot FROM bots_statistics WHERE time_start='%s'" % time_start
        rows = self.session.execute(query)
        return rows

    def query3(self, time_start):
        """
        type A № 3
        Return Top 20 users that created the most pages during the last 6 hours, excluding the last hour.
        The response should contain username, user id, start and end time, the list of the page titles,
        and the number of pages created.
        :param time_start: time in format hh:mm:ss
        :return: Rows
        """
        query = "SELECT user_id, user_name, page_count, pages_list FROM users_statistics WHERE " \
                "time_start='%s'" % time_start
        rows = self.session.execute(query)
        return rows

    def query4(self):
        """
        type B № 1
        Return the list of existing domains for which pages were created.
        :return: Rows
        """
        query = "SELECT domain FROM page_creation"
        rows = self.session.execute(query)
        return rows

    def query5(self, user_id):
        """
        type B № 2
        Return all the pages which were created by the user with a specified user_id.
        :param user_id: string with user id
        :return: Rows
        """
        query = "SELECT page_url FROM users_pages WHERE user_id='%s'" % user_id
        rows = self.session.execute(query)
        return rows

    def query6(self, domain_name):
        """
        type B № 3
        Return the number of articles created for a specified domain.
        :param domain_name: string with domain name
        :return: Rows
        """
        time_now = datetime.datetime.now()
        big_time = time_now + datetime.timedelta(days=365)
        query = "SELECT COUNT(*) AS count FROM page_creation WHERE time_created < '%s' AND domain = '%s' " \
                "ALLOW FILTERING"\
                % (big_time.strftime("%Y-%m-%d %H:%M:%S+0000"), domain_name)
        rows = self.session.execute(query)
        return rows

    def query7(self, page_id):
        """
        type B № 4
        Return the page with the specified page_id
        :param page_id: string with page id
        :return: Rows
        """
        query = "SELECT page_url FROM pages WHERE page_id='%s'" % page_id
        rows = self.session.execute(query)
        return rows

    def query8(self, time_start, time_end):
        """
        type B № 5
        Return the id, name, and the number of created pages of all the users who created at least one page
        in a specified time range.
        :param time_start: time in format hh:mm:ss
        :param time_end: time in format hh:mm:ss
        :return: Rows
        """
        query = "SELECT user_id, user_name FROM page_creation WHERE time_created >= '%s' AND time_created <= '%s'" \
                "ALLOW FILTERING" % (time_start, time_end)
        rows = self.session.execute(query)
        return rows


class CassandraAPI:

    def __init__(self, name="CassandraAPI"):
        self.app = Flask(name)
        self.app.config['JSON_SORT_KEYS'] = False
        self.name = name
        self.client = self.create_client()
        self.client.connect()

        @self.app.route('/', methods=['GET'])
        def get_request():
            if request.method == 'GET':
                query_body = request.get_json()
                try:
                    result = self.execute_query(query_body)
                    return jsonify(result)
                except AttributeError as err:
                    return jsonify(err=str(err)), 400

    def run(self, host, port):
        self.app.run(host=host, port=port)

    def __del__(self):
        self.client.close()

    def execute_query(self, query_body):
        if len(query_body.keys()) != 3 or "query_type" not in query_body.keys()\
                or "query_number" not in query_body.keys() or "params" not in query_body.keys():
            raise AttributeError("Wrong body: it should only have fields 'query_type','query_number' and 'params'")

        query_type = query_body["query_type"]
        if query_type != "A" and query_type != "B":
            raise AttributeError("Wrong query_type: it should be 'A' OR 'B'")

        if query_type == "A":

            try:
                query_number = int(query_body["query_number"])
            except ValueError:
                raise AttributeError("Wrong type A query_number: it should be int between 1 and 3")
            if query_number < 1 or query_number > 3:
                raise AttributeError("Wrong type A query_number: it should be int between 1 and 3")

            params = query_body["params"]

            if len(params.keys()) != 0:
                raise AttributeError("Wrong type A query params: params field should be empty")

            time_now = datetime.datetime.now(pytz.utc)
            start_time = time_now.replace(minute=0, second=0,  microsecond=0) - datetime.timedelta(hours=7)

            if query_number == 1:
                result_json = {"hours": []}
                for i in range(6):
                    time_start = start_time + datetime.timedelta(hours=i)
                    time_end = time_start + datetime.timedelta(hours=1)
                    rows = self.client.query1(time_start.strftime("%Y-%m-%d %H:%M:%S+0000"))
                    cur_statistics = {"time_start": time_start.strftime("%H:%M"),
                                      "time_end": time_end.strftime("%H:%M"),
                                      "statistics": []}
                    for row in rows:
                        cur_statistics["statistics"].append({str(row.domain): int(row.all_pages)})
                    result_json["hours"].append(cur_statistics)
            elif query_number == 2:
                end_time = start_time + datetime.timedelta(hours=6)
                rows = self.client.query2(start_time.strftime("%Y-%m-%d %H:%M:%S+0000"))
                result_json = {"time_start": start_time.strftime("%H:%M"),
                               "time_end": end_time.strftime("%H:%M"),
                               "statistics": []}
                for row in rows:
                    result_json["statistics"].append({"domain": str(row.domain),
                                                      "created_by_bots": int(row.created_by_bot)})
            else:
                end_time = start_time + datetime.timedelta(hours=6)
                rows = self.client.query3(start_time.strftime("%Y-%m-%d %H:%M:%S+0000"))
                result_json = {"time_start": start_time.strftime("%H:%M"),
                               "time_end": end_time.strftime("%H:%M"),
                               "users_statistics": []}
                for row in rows:
                    user_stat = {"user_id": str(row.user_id),
                                 "user_name": str(row.user_name),
                                 "page_count": int(row.page_count),
                                 "pages_titles": row.pages_list}
                    result_json["users_statistics"].append(user_stat)
        else:

            try:
                query_number = int(query_body["query_number"])
            except ValueError:
                raise AttributeError("Wrong type B query_number: it should be int between 1 and 5")
            if query_number < 1 or query_number > 5:
                raise AttributeError("Wrong type B query_number: it should be int between 1 and 5")

            params = query_body["params"]
            if query_number == 1:

                if len(params.keys()) != 0:
                    raise AttributeError("Wrong params for type B query %d: it should have empty params"
                                         % query_number)

                rows = self.client.query4()
                result_json = {"domains": []}
                for row in rows:
                    if str(row.domain) not in result_json["domains"]:
                        result_json["domains"].append(str(row.domain))

            elif query_number == 2:

                if len(params.keys()) != 1 or "user_id" not in params.keys():
                    raise AttributeError("Wrong params for type B query %d: it should only have 'user_id'"
                                         % query_number)

                user_id = params["user_id"]
                rows = self.client.query5(user_id)
                result_json = {"pages": []}
                for row in rows:
                    result_json["pages"].append(str(row.page_url))

            elif query_number == 3:

                if len(params.keys()) != 1 or "domain_name" not in params.keys():
                    raise AttributeError("Wrong params for type B query %d: it should only have 'domain_name'"
                                         % query_number)

                domain_name = params["domain_name"]
                rows = self.client.query6(domain_name)
                result_json = {"number_of_pages": int(rows[0].count)}

            elif query_number == 4:

                if len(params.keys()) != 1 or "page_id" not in params.keys():
                    raise AttributeError("Wrong params for type B query %d: it should only have 'page_id'"
                                         % query_number)

                page_id = params["page_id"]
                rows = self.client.query7(page_id)
                if not rows:
                    result_json = {"url": ""}
                else:
                    result_json = {"url": rows[0].page_url}

            else:
                if len(params.keys()) != 2 or "time_start" not in params.keys() or "time_end" not in params.keys():
                    raise AttributeError("Wrong params for type B query %d: it should only have 'time_start' and "
                                         "'time_end'"
                                         % query_number)

                time_start = params["time_start"]
                time_end = params["time_end"]

                try:
                    datetime.datetime.strptime(time_start, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    raise AttributeError("Wrong params for type B query %d: field 'time_start' should be in format "
                                         "'hh:mm:ss'"
                                         % query_number)
                try:
                    datetime.datetime.strptime(time_end, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    raise AttributeError("Wrong params for type B query %d: field 'time_end' should be in format "
                                         "'hh:mm:ss'"
                                         % query_number)

                rows = self.client.query8(time_start, time_end)
                result_json = {"users": []}
                users = {}
                for row in rows:
                    if str(row.user_id) in users.keys():
                        users[str(row.user_id)]["pages"] += 1
                    else:
                        users[str(row.user_id)] = {"user_name": str(row.user_name), "pages": 1}
                for user_id in users.keys():
                    user = {"user_id": user_id,
                            "user_name": users[user_id]["user_name"],
                            "page_count": users[user_id]["pages"]}
                    result_json["users"].append(user)
        return result_json

    @staticmethod
    def create_client():
        host = 'cassandra-node'
        port = 9042
        keyspace = 'wikipedia_dulher'

        client = CassandraClient(host, port, keyspace)
        return client


def main():
    app = CassandraAPI()
    app.run("0.0.0.0", 8080)


if __name__ == '__main__':
    main()
