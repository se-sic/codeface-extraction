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
# Copyright 2018 by Anselm Fehnker <fehnker@fim.uni-passau.de>
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
from datetime import datetime

import operator
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
    print_to_disk_new(issues, __resdir)

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

    def format_time(time):
        """Tries to format times from different sources to a consistent time format

        :param time: the time that shall be formatted
        :return: the formatted time
        """

        try:
            # format from new GitHub-Wrapper files
            d = datetime.strptime(time, '%Y-%m-%dT%H:%M:%SZ')
            return d.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            # format from new GitHub-Wrapper files with no seconds
            try:
                d = datetime.strptime(time, '%Y-%m-%dT%H:%MZ')
                return d.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    # format of relatedCommits time
                    d = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S+%f:00')
                    return d.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    # if the given time format is not know it can't be formatted, so it just gets returned
                    # also happens if the time already has the correct format (old GitHub-Wrapper files)
                    return time

    # known types from JIRA and GitHub default labels
    known_types = {"bug", "improvement", "enhancement", "new feature", "task", "test", "wish"}

    # known resolutions from JIRA and GitHub default labels
    known_resolutions = {"unresolved", "fixed", "wontfix", "duplicate", "invalid", "incomplete",
                               "cannot reproduce", "later", "not a problem", "implemented", "done",
                               "auto closed",
                               "pending", "closed", "remind", "resolved", "not a bug", "workaround",
                               "staged",
                               "delivered", "information provided", "works for me", "feedback received",
                               "wontdo"}

    # re-process all issues
    for issue in issue_data:
        # temporary container for references
        comments = dict()

        # empty container for issue types
        issue["type"] = []

        # empty container for issue resolutions
        issue["resolution"] = []

        # if an issue has no eventsList an empty Lists gets created
        if issue['eventsList'] is None:
            issue['eventsList'] = []

        # if an issue has no commentsList an empty Lists gets created
        if issue["commentsList"] is None:
            issue["commentsList"] = []

        # if an issue has no relatedCommits an empty Lists gets created
        if issue["relatedCommits"] is None:
            issue["relatedCommits"] = []

        # if an issue has no relatedIssues an empty Lists gets created
        if "relatedIssues" not in issue:
            issue["relatedIssues"] = []

        # add 'closed_at' information if not present yet
        if issue["closed_at"] is None:
            issue["closed_at"] = ""

        # parses the creation time in the correct format
        issue['created_at'] = format_time(issue['created_at'])

        # parses the close time in the correct format
        issue['closed_at'] = format_time(issue['closed_at'])

        # checks if the issue is a pull-request or a normal issue and adapts the type
        if issue["isPullRequest"]:
            issue["type"].append("pull request")
        else:
            issue["type"].append("issue")

        # brings the state to lowercase. Is needed for consistency reasons in the new file format
        issue["state_new"] = issue["state"].lower()

        # adds the issue creation time with the default state to an list
        # list is needed to find out the state the issue had when a comment was written
        state_changes = [[issue['created_at'], "open"]]

        # adds the issue creation time with the default resolution to an list
        # list is needed to find out the resolutions the issue had when a comment was written
        resolution_changes = [[issue['created_at'], []]]

        # adds creation event to eventsList
        created_event = dict()
        created_event["user"] = issue["user"]
        created_event["created_at"] = issue["created_at"]
        created_event["event"] = "created"
        created_event["event_info_1"] = "open"
        created_event["event_info_2"] = []
        issue["eventsList"].append(created_event)

        # adds events for related issues to eventsList
        # GH-Wrapper doesn't provide information about time and user yet. Has to be updated if this changes
        for rel_issue in issue["relatedIssues"]:
            link_event = dict()
            author = dict()
            author["username"] = ""
            author["name"] = ""
            author["email"] = ""
            link_event["user"] = author
            link_event["created_at"] = ""
            link_event["event"] = "add_link"
            link_event["event_info_1"] = rel_issue
            link_event["event_info_2"] = "issue"
            link_event["ref_target"] = ""
            issue["eventsList"].append(link_event)

        # adds events for related commits to eventsList
        for rel_commit in issue["relatedCommits"]:
            link_event = dict()
            link_event["created_at"] = rel_commit["time"]
            link_event["event"] = "add_link"
            link_event["event_info_1"] = rel_commit["hash"]
            link_event["event_info_2"] = "commit"
            link_event["ref_target"] = ""

            # old gh-wrapper data has just a unicode string as an author. new gh-wrapper data has an author object
            # for old data an author object is created
            if isinstance(rel_commit["author"], unicode):
                author = dict()
                author["username"] = rel_commit["author"]
                author["name"] = rel_commit["author"]
                author["email"] = ""
                link_event["user"] = author
            else:
                link_event["user"] = rel_commit["author"]

            issue["eventsList"].append(link_event)

        # updates format of every event in the eventsList of an issue
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

            # parses the event time in the correct format
            event["created_at"] = format_time(event["created_at"])

            # formats the event according to it's event type
            if event["event"] == "closed":
                event["event"] = "state_updated"
                event["event_info_1"] = "closed"
                event["event_info_2"] = "open"
                # creates new state change object
                state_changes.append([event['created_at'], "closed"])

            elif event["event"] == "reopened":
                event["event"] = "state_updated"
                event["event_info_1"] = "open"
                event["event_info_2"] = "closed"
                # creates new resolution change object
                state_changes.append([event['created_at'], "open"])

            elif event["event"] == "labeled":
                label = event["label"]["name"].lower()
                event["event_info_1"] = label

                # if the label is in this list, it also is a type of the issue
                if label in known_types:
                    issue["type"].append(str(label))

                    # creates an event for type updates and adds it to the eventsList
                    type_event = dict()
                    type_event["user"] = issue["user"]
                    type_event["created_at"] = event["created_at"]
                    type_event["event"] = "type_updated"
                    type_event["event_info_1"] = label
                    type_event["event_info_2"] = ""
                    type_event["ref_target"] = ""
                    issue["eventsList"].append(type_event)

                # if the label is in this list, it also is a resolution of the issue
                elif label in known_resolutions:
                    issue["resolution"].append(str(label))

                    # creates an event for resolution updates and adds it to the eventsList
                    resolution_event = dict()
                    resolution_event["user"] = issue["user"]
                    resolution_event["created_at"] = event["created_at"]
                    resolution_event["event"] = "resolution_updated"
                    resolution_event["event_info_1"] = label
                    resolution_event["event_info_2"] = ""
                    resolution_event["ref_target"] = ""
                    issue["eventsList"].append(resolution_event)

                    # the added resolution is appended to the old resolution list
                    # works because label events are sorted by time
                    new_resolutions = list(resolution_changes[-1][1])
                    new_resolutions.append(str(label))
                    # creates new resolution change object
                    resolution_changes.append([resolution_event["created_at"], new_resolutions])

            # %TODO: event for removed labels has to be checked. Data is missing

            # if event_info_1 does not exist in the event, an empty info is created
            if "event_info_1" not in event:
                event["event_info_1"] = ""

            # if event_info_2 does not exist in the event, an empty info is created
            if 'event_info_2' not in event:
                event["event_info_2"] = ""

        # state and resolution change lists get sorted by time
        state_changes.sort(key=lambda x: x[0])
        resolution_changes.sort(key=lambda x: x[0])

        # add event name to comment and add reference target
        for comment in issue["commentsList"]:
            comment["event"] = "commented"
            comment["ref_target"] = ""

            comment["created_at"] = format_time(comment["created_at"])

            # cache comment by date to resolve/re-arrange references later
            comments[comment["created_at"]] = comment

            # the state the issue had when the comment was written is searched out
            for state in state_changes:
                if comment['created_at'] > state[0]:
                    comment['event_info_1'] = state[1]

            # the resolution the issue had when the comment was written is searched out
            for resolution in resolution_changes:
                if comment['created_at'] > resolution[0]:
                    comment['event_info_2'] = resolution[1]

        # merge events and comment lists
        issue["eventsList"] = issue["commentsList"] + issue["eventsList"]

        # remove events without user
        issue["eventsList"] = [event for event in issue["eventsList"] if
                               not (event["user"] is None or event["ref_target"] is None)]

        # sorts eventsList by time
        issue["eventsList"].sort(key=operator.itemgetter('created_at'))

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
                event["event_info_1"] = event["ref_target"]["name"]
                event["event_info_2"] = event["ref_target"]["email"]

    return issues


def print_to_disk(issues, results_folder):
    """Print issues to file 'issues.list' in result folder.
    This format is outdated but still used by the network library.
    When the network library is updated, this method can be overwritten by 'print_to_disk_new'.

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


def print_to_disk_new(issues, results_folder):
    """Print issues to file 'issues_new.list' in result folder.
    This file has a consistent format to the 'bugs-jira.list' file.
    When the network library is updated, this is the format which shall be used.

    :param issues: the issues to dump
    :param results_folder: the folder where to place 'issues.list' output file
    """

    # construct path to output file
    output_file = os.path.join(results_folder, "issues_new.list")
    log.info("Dumping output in file '{}'...".format(output_file))

    # construct lines of output
    lines = []
    for issue in issues:
        for event in issue["eventsList"]:
            lines.append((
                issue["number"],
                issue["type"],
                issue["state_new"],
                issue["resolution"],
                issue["created_at"],
                issue["closed_at"],
                [],  # components
                event["event"],
                event["user"]["name"],
                event["user"]["email"],
                event["created_at"],
                event["event_info_1"],
                event["event_info_2"]
            ))

    # write to output file
    csv_writer.write_to_csv(output_file, lines)
