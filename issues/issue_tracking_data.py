import argparse
import sys
import json
import urllib
import urllib2
import httplib
import time
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
    time.sleep(2)
    __codeface_conf, __project_conf = map(abspath, (args.config, args.project))
    __conf = Configuration.load(__codeface_conf, __project_conf)
    _dbm = DBManager(__conf)
    __resdir = abspath(args.resdir + "/" + __conf['repo'] + "_issues")
    project_id = _dbm.getProjectID(__conf["project"], __conf["tagging"])
    issues = load(__resdir)
    issues = insert_user_ids(issues, project_id, __conf)
    issues = reformat(issues)
    print_to_disk(issues, __resdir)
    return None


def load(file_path):
    with open(file_path + "/issues.json") as issues_file:
        data = json.load(issues_file)
    return data


def insert_user_ids(issues, project_id, __conf):
    user_buffer = dict()
    for issue in issues:
        # check database for event authors
        for event in issue["eventsList"]:
            if event["user"] is None:
                issue["eventsList"].remove(event)
            else:
                if event["user"]["username"] not in user_buffer:
                    event["user"] = verify_user(event["user"], project_id, __conf)
                    user_buffer[event["user"]["username"]] = event["user"]
                else:
                    event["user"] = user_buffer[event["user"]["username"]]

        # Check database for comment authors
        for comment in issue["commentsList"]:
            if comment["user"] is None:
                issue["commentsList"].remove(comment)
            else:
                if comment["user"]["username"] not in user_buffer:
                    comment["user"] = verify_user(comment["user"], project_id, __conf)
                    user_buffer[comment["user"]["username"]] = comment["user"]
                else:
                    comment["user"] = user_buffer[comment["user"]["username"]]

        # check database for issue authors
        if issue["user"]["username"] not in user_buffer:
            issue["user"] = verify_user(issue["user"], project_id, __conf)
            user_buffer[issue["user"]["username"]] = issue["user"]
        else:
            issue["user"] = user_buffer[issue["user"]["username"]]
    return issues


def verify_user(user, project_id, __conf):
    mail = unicode(user["email"]).encode("utf-8")
    if user["name"] is not None:
        name = unicode(user["name"]).encode("utf-8")
    else:
        name = unicode(user["username"]).encode("utf-8")

    params = urllib.urlencode({"projectID": project_id, "name": name, "email": mail})
    conn = httplib.HTTPConnection(__conf['idServiceHostname'], __conf['idServicePort'])
    headers = {"Content-type": "application/x-www-form-urlencoded; charset=utf-8", "Accept": "text/plain"}
    conn.request("POST", "/post_user_id", params, headers)
    response = conn.getresponse()
    user_id = response.read()
    user_id = json.loads(user_id)
    url = "/getUser/" + str(user_id["id"])
    conn.request("GET", url, headers=headers)
    response = conn.getresponse()
    db_user = json.loads(response.read())[0]

    user["email"] = db_user["email1"]
    user["name"] = db_user["name"]
    conn.close()
    print user
    return user


def reformat(issues):
    for issue in issues:
        if issue["closed_at"] is None:
            issue["closed_at"] = ""

        dates = dict()
        created_event = dict()
        created_event["user"] = issue["user"]
        created_event["created_at"] = issue["created_at"]
        created_event["event"] = "created"
        issue["eventsList"].append(created_event)

        for comment in issue["commentsList"]:
            comment["event"] = "commented"
            dates[comment["created_at"]] = comment
            comment["ref_target"] = ""

        for event in issue["eventsList"]:
            event["ref_target"] = ""
            if event["created_at"] in dates:
                check_event(event, dates[event["created_at"]])

        issue["eventsList"] = issue["commentsList"] + issue["eventsList"]



    return issues


def check_event(event1, event2):
    if event1["event"] == "mentioned" or event1["event"] == "subscribed":
        event1["ref_target"] = event1["user"]
        event1["user"] = event2["user"]
    return event1


def print_to_disk(issues, file_path):
    with open(file_path + "/issues.list", 'w+') as output_file:
        for issue in issues:
            for event in issue["eventsList"]:
                output_file.write((str(issue["number"]) + ";"))
                output_file.write('"{}"'.format(issue["state"]) + ";")
                output_file.write('"{}"'.format(issue["created_at"]) + ";")
                output_file.write('"{}"'.format(issue["closed_at"]) + ";")
                output_file.write(str(issue["isPullRequest"]) + ";")
                output_file.write('"{}"'.format((event["user"]["name"] + ";").encode("utf-8")))
                output_file.write('"{}"'.format(event["created_at"]) + ";")
                output_file.write(('"{}"'.format("") if event["ref_target"] == ""
                                   else '"{}"'.format((event["ref_target"]["name"]).encode("utf-8"))) + ";")
                output_file.write('"{}"'.format(event["event"]))
                output_file.write("\n")
