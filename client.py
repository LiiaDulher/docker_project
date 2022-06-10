import datetime

import pytz
import requests
import pprint


class Client:
    def __init__(self, url):
        self.url = url
        self.queries = {
            1: [
            ],
            2: [
                {
                    "name": "user_id",
                    "help_string": "string with user id"
                }
            ],
            3: [
                {
                    "name": "domain_name",
                    "help_string": "string with name of the domain"
                }
            ],
            4: [
                {
                    "name": "page_id",
                    "help_string": "string with page id"
                }
            ],
            5: [
                {
                    "name": "time_start",
                    "help_string": "time in format %Y-%m-%d %H:%M:%S"
                },
                {
                    "name": "time_end",
                    "help_string": "time in format %Y-%m-%d %H:%M:%S"
                }
            ]
        }

    def get_request(self):
        query_type = input("A: pre-computed queries\n"
                           "B: ad-hoc queries\n"
                           "0: exit\n")
        query_body = {
            "query_type": "",
            "query_number": 0,
            "params": {}
        }
        if query_type == "0":
            return None
        elif query_type == "A":
            query_number = int(input("1: Return the aggregated statistics containing the number of created pages for "
                                     "each Wikipedia domain for each hour in the last 6 hours, excluding the last "
                                     "hour.\n"
                                     "2: Return the statistics about the number of pages created by bots for each of "
                                     "the domains for the last 6 hours, excluding the last hour. \n"
                                     "3: Return Top 20 users that created the most pages during the last 6 "
                                     "hours, excluding the last hour.\n"
                                     "0: exit\n"))
            if query_number == 0:
                return None
            if 0 < query_number < 4:
                query_body["query_number"] = query_number
                query_body["query_type"] = "A"
                return query_body
        else:
            query_number = int(input("1: Return the list of existing domains for which pages were created.\n"
                                     "2: Return all the pages which were created by the user with a specified user_id\n"
                                     "3: Return the number of articles created for a specified domain.\n"
                                     "4: Return the page with the specified page_id\n"
                                     "5: Return the id, name, and the number of created pages of all the users who "
                                     "created at least one page in a specified time range \n"
                                     "0: exit\n"))
            if query_number == 0:
                return None
            if 0 < query_number < 6:
                query_body["query_number"] = query_number
                query_body["query_type"] = "B"
                for param in self.queries[query_number]:
                    p = input("Enter %s (%s):" % (param["name"], param["help_string"]))
                    query_body["params"][param["name"]] = p
                return query_body

        raise AttributeError("Wrong query number")

    def send_query(self, query_body):
        try:
            response = requests.get(self.url, json=query_body)
        except requests.exceptions.RequestException as err:
            return 0, err
        return response.status_code, response.json()


def main():
    host = 'http://localhost'
    port = 8080
    url = host + ":" + str(port)
    client = Client(url)
    print("Current time in UTC:", datetime.datetime.now(pytz.utc))
    while True:
        try:
            query_body = client.get_request()
        except AttributeError as err:
            print(err)
            continue
        if query_body is None:
            break
        else:
            pprint.pprint(query_body)
            code, json = client.send_query(query_body)
            print("Code: %d" % code)
            pprint.pprint(json)


if __name__ == "__main__":
    main()
