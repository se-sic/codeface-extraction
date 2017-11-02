import argparse
import csv
import httplib
import json
import sys
import time
import urllib
from os.path import abspath

from codeface.cli import log
from codeface.configuration import Configuration
from codeface.dbmanager import DBManager


def run():
    parser = argparse.ArgumentParser(prog='codeface', description='Codeface extraction')
    parser.add_argument('-c', '--config', help="Codeface configuration file", default='codeface.conf')
    parser.add_argument('-p', '--project', help="Project configuration file", required=True)
    parser.add_argument('resdir', help="Directory to store analysis results in")
    args = parser.parse_args(sys.argv[1:])
    __codeface_conf, __project_conf = map(abspath, (args.config, args.project))
    __conf = Configuration.load(__codeface_conf, __project_conf)
    _dbm = DBManager(__conf)
    __resdir = abspath(args.resdir + "/" + __conf['repo'] + "_issues")
    project_id = _dbm.getProjectID(__conf["project"], __conf["tagging"])
    issues = load(__resdir)
    issues = reformat(issues)
    issues = insert_user_data(issues, project_id, __conf)
    print_to_disk(issues, __resdir)
    return None


def load(file_path):
    with open(file_path + "/issues.json") as issues_file:
        data = json.load(issues_file)
    return data


def reformat(issues):
    for issue in issues:
        if issue["closed_at"] is None:
            issue["closed_at"] = ""

        # add created event
        dates = dict()
        created_event = dict()
        created_event["user"] = issue["user"]
        created_event["created_at"] = issue["created_at"]
        created_event["event"] = "created"
        issue["eventsList"].append(created_event)

        # add event name to comment and add reference target
        for comment in issue["commentsList"]:
            comment["event"] = "commented"
            dates[comment["created_at"]] = comment
            comment["ref_target"] = ""

        # add reference target to events
        for event in issue["eventsList"]:
            event["ref_target"] = ""
            if event["created_at"] in dates:
                check_event(event, dates[event["created_at"]])
        # merge events and comment lists
        issue["eventsList"] = issue["commentsList"] + issue["eventsList"]

        # remove events without user
        event_list = []
        for event in issue["eventsList"]:
            if not (event["user"] is None or event["ref_target"] is None):
                event_list.append(event)
        issue["eventsList"] = event_list

    return issues


def check_event(event1, comment):
    if (event1["event"] == "mentioned" or event1["event"] == "subscribed") and comment["event"] == "commented":
        event1["ref_target"] = event1["user"]
        event1["user"] = comment["user"]
    return event1


def insert_user_data(issues, project_id, __conf):
    user_buffer = dict()
    for issue in issues:
        # check database for event authors
        for event in issue["eventsList"]:
            if event["user"]["username"] not in user_buffer:
                event["user"] = check_user(event["user"], project_id, __conf)
                user_buffer[event["user"]["username"]] = event["user"]
            else:
                event["user"] = user_buffer[event["user"]["username"]]
            if event["ref_target"] != "" and event["ref_target"]["username"] not in user_buffer:
                event["ref_target"] = check_user(event["ref_target"], project_id, __conf)
                user_buffer[event["ref_target"]["username"]] = event["ref_target"]
            elif event["ref_target"] != "":
                event["ref_target"] = user_buffer[event["ref_target"]["username"]]

        # check database for issue authors
        if issue["user"]["username"] not in user_buffer:
            issue["user"] = check_user(issue["user"], project_id, __conf)
            user_buffer[issue["user"]["username"]] = issue["user"]
        else:
            issue["user"] = user_buffer[issue["user"]["username"]]
    return issues


def check_user(user, project_id, __conf):
    if user["name"] is not None:
        name = unicode(user["name"]).encode("utf-8")
    else:
        name = unicode(user["username"]).encode("utf-8")
    mail = unicode(user["email"]).encode("utf-8")

    conn = httplib.HTTPConnection(__conf['idServiceHostname'], __conf['idServicePort'])
    headers = {"Content-type": "application/x-www-form-urlencoded; charset=utf-8", "Accept": "text/plain"}
    params = urllib.urlencode({"projectID": project_id, "name": name, "email": mail})
    conn.request("POST", "/post_user_id", params, headers)
    response = conn.getresponse()
    user_id = response.read()
    user_id = json.loads(user_id)
    if "error" in user_id:
        mail = name + "@default.com"
        params = urllib.urlencode({"projectID": project_id, "name": name, "email": mail})
        conn.request("POST", "/post_user_id", params, headers)
        response = conn.getresponse()
        user_id = json.loads(response.read())
    url = "/getUser/" + str(user_id["id"])
    conn.request("GET", url, headers=headers)
    response = conn.getresponse()
    db_user = json.loads(response.read())[0]

    user["email"] = db_user["email1"]
    user["name"] = db_user["name"]
    conn.close()
    return user


def print_to_disk(issues, file_path):
    with open(file_path + "/issues.list", 'wb') as output_file:
        wr = csv.writer(output_file, delimiter=';', lineterminator='\n', quoting=csv.QUOTE_NONNUMERIC)
        for issue in issues:
            for event in issue["eventsList"]:
                wr.writerow((
                    issue["number"], issue["state"], issue["created_at"], issue["closed_at"], issue["isPullRequest"],
                    (event["user"]["name"]).encode("utf-8"), (event["user"]["email"]).encode("utf-8"),
                    event["created_at"],
                    "" if event["ref_target"] == "" else (event["ref_target"]["name"]).encode("utf-8"),
                    event["event"]))
