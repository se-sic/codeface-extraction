# coding=utf-8
# This file is part of codeface-extraction, which is free software: you
# can redistribute it and/or modify it under the terms of the GNU General
# Public License as published by the Free Software Foundation, version 2.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Copyright 2017 by Raphael Nömmer <noemmer@fim.uni-passau.de>
# Copyright 2017 by Claus Hunsen <hunsen@fim.uni-passau.de>
# Copyright 2018 by Barbara Eckl <ecklbarb@fim.uni-passau.de>
# All Rights Reserved.
"""
This file is able to extract Github issue data from json files.
"""

import argparse
import httplib
import json
import os
import sys
import urllib

from codeface.cli import log
from codeface.cluster.idManager import idManager
from codeface.configuration import Configuration
from codeface.dbmanager import DBManager

from csv_writer import csv_writer


def run():
    # get all needed paths and argument for the method call.
    parser = argparse.ArgumentParser(prog='codeface', description='Codeface extraction')
    parser.add_argument('-c', '--config', help="Codeface configuration file", default='codeface.conf')
    parser.add_argument('-p', '--project', help="Project configuration file", required=True)
    parser.add_argument('resdir', help="Directory to store analysis results in")

    # parse arguments
    args = parser.parse_args(sys.argv[1:])
    __codeface_conf, __project_conf = map(os.path.abspath, (args.config, args.project))

    # create configuration
    __conf = Configuration.load(__codeface_conf, __project_conf)

    # get source and results folders
    __srcdir = os.path.abspath(os.path.join(args.resdir, __conf['repo'] + "_issues"))
    __resdir = os.path.abspath(os.path.join(args.resdir, __conf['project'], __conf["tagging"]))

    # run processing of issue data:
    # 1) load the list of issues
    issues = load(__srcdir)
    # 2) re-format the issues
    issues = reformat(issues)
    # 3) update user data with Codeface database
    issues = insert_user_data(issues, __conf)
    # 4) dump result to disk
    print_to_disk(issues, __resdir)

    log.info("Github issue processing complete!")


def load(source_folder):
    """Load issues from disk.

    :param source_folder: the folder where to find 'issues.json'
    :return: the loaded issue data
    """

    srcfile = os.path.join(source_folder, "issues.json")
    log.devinfo("Loading Github issues from file '{}'...".format(srcfile))

    # check if file exists and exit early if not
    if not os.path.exists(srcfile):
        log.error("Github issue file '{}' does not exist! Exiting early...".format(srcfile))
        sys.exit(-1)

    with open(srcfile) as issues_file:
        issue_data = json.load(issues_file)

    return issue_data


def reformat(issue_data):
    """Re-arrange issue data structure.

    :param issue_data: the issue data to re-arrange
    :return: the re-arranged issue data
    """

    log.devinfo("Re-arranging Github issues...")

    # re-process all issues
    for issue in issue_data:
        # temporary container for references
        comments = dict()

        # initialize event
        created_event = dict()
        created_event["user"] = issue["user"]
        created_event["created_at"] = issue["created_at"]
        created_event["event"] = "created"
        issue["eventsList"].append(created_event)

        # add event name to comment and add reference target
        for comment in issue["commentsList"]:
            comment["event"] = "commented"
            comment["ref_target"] = ""
            # cache comment by date to resolve/re-arrange references later
            comments[comment["created_at"]] = comment

        # add reference target to events
        for event in issue["eventsList"]:
            event["ref_target"] = ""
            # if event collides with a comment
            if event["created_at"] in comments:
                comment = comments[event["created_at"]]
                # if someone gets mentioned or subscribed by someone else in a comment,
                # re-write the reference
                if (event["event"] == "mentioned" or event["event"] == "subscribed") and \
                                comment["event"] == "commented":
                    event["ref_target"] = event["user"]
                    event["user"] = comment["user"]

        # merge events and comment lists
        issue["eventsList"] = issue["commentsList"] + issue["eventsList"]

        # add 'closed_at' information if not present yet
        if issue["closed_at"] is None:
            issue["closed_at"] = ""

        # remove events without user
        issue["eventsList"] = [event for event in issue["eventsList"] if
                               not (event["user"] is None or event["ref_target"] is None)]

    return issue_data


def insert_user_data(issues, conf):
    """Insert user data into database ad update issue data.

    :param issues: the issues to retrieve user data from
    :param conf: the project configuration
    :return: the updated issue data
    """

    log.info("Syncing users with ID service...")

    # create buffer for users
    user_buffer = dict()
    # open database connection
    dbm = DBManager(conf)
    # open ID-service connection
    idservice = idManager(dbm, conf)

    def get_user_string(name, email):
        if not email or email is None:
            return "{name}".format(name=name)
            # return "{name} <{name}@default.com>".format(name=name)  # for debugging only
        else:
            return "{name} <{email}>".format(name=name, email=email)

    def get_or_update_user(user, buffer_db=user_buffer):
        # fix encoding for name and e-mail address
        if user["name"] is not None:
            name = unicode(user["name"]).encode("utf-8")
        else:
            name = unicode(user["username"]).encode("utf-8")
        mail = unicode(user["email"]).encode("utf-8")
        # construct string for ID service and send query
        user_string = get_user_string(name, mail)

        # check buffer to reduce amount of DB queries
        if user_string in buffer_db:
            log.devinfo("Returning user '{}' from buffer.".format(user_string))
            return buffer_db[user_string]

        # get person information from ID service
        log.devinfo("Passing user '{}' to ID service.".format(user_string))
        idx = idservice.getPersonID(user_string)

        # update user data with person information from DB
        person = idservice.getPersonFromDB(idx)
        user["email"] = person["email1"]  # column 'email1'
        user["name"] = person["name"]  # column 'name'
        user["id"] = person["id"]  # column 'id'

        # add user information to buffer
        # user_string = get_user_string(user["name"], user["email"]) # update for
        buffer_db[user_string] = user

        return user

    for issue in issues:
        # check database for issue author
        issue["user"] = get_or_update_user(issue["user"])

        # check database for event authors
        for event in issue["eventsList"]:
            # get the event user from the DB
            event["user"] = get_or_update_user(event["user"])
            # get the reference-target user from the DB if needed
            if event["ref_target"] != "":
                event["ref_target"] = get_or_update_user(event["ref_target"])

    return issues


def print_to_disk(issues, results_folder):
    """Print issues to file 'issues.list' in result folder

    :param issues: the issues to dump
    :param results_folder: the folder where to place 'issues.list' output file
    """

    # construct path to output file
    output_file = os.path.join(results_folder, "issues.list")
    log.info("Dumping output in file '{}'...".format(output_file))

    # construct lines of output
    lines = []
    for issue in issues:
        for event in issue["eventsList"]:
            lines.append((
                issue["number"],
                issue["state"],
                issue["created_at"],
                issue["closed_at"],
                issue["isPullRequest"],
                event["user"]["name"],
                event["user"]["email"],
                event["created_at"],
                "" if event["ref_target"] == "" else event["ref_target"]["name"],
                event["event"]
            ))

    # write to output file
    csv_writer.write_to_csv(output_file, lines)
