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
This file is able to extract Jira issue data from xml files.
"""

import argparse
import os
import sys
import time
import csv

from xml.dom.minidom import parse
from datetime import datetime

from codeface.cli import log
from codeface.cluster.idManager import idManager
from codeface.configuration import Configuration
from codeface.dbmanager import DBManager

from csv_writer import csv_writer

from jira import JIRA

reload(sys)
sys.setdefaultencoding('utf-8')
with_api = True  # flag to choose if information via api shall be loaded. Increases runtime
project_url = 'https://issues.apache.org/jira'  # needed for api


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
    __srcdir = os.path.abspath(os.path.join(args.resdir, __conf['repo'] + "_proximity", "conway", "issues_xml"))
    __resdir = os.path.abspath(os.path.join(args.resdir, __conf['project'], __conf["tagging"]))
    __srcdir_csv = os.path.abspath(os.path.join(args.resdir, __conf['repo'] + "_proximity", "conway"))

    # get person folder
    # __psrcdir = os.path.abspath(os.path.join(args.resdir, __conf['repo'] + "_proximity", "conway"))

    # 1) load the list of issues
    issues = load_xml(__srcdir)
    # 1b) load the list of persons
    persons = load_csv(__srcdir_csv)
    # 2) re-format the issues
    issues = parse_xml(issues, persons)
    # 3) load issue information via api
    if with_api:
        load_issue_via_api(issues, persons)
    # 4) update user data with Codeface database
    # mabye not nessecary
    issues = insert_user_data(issues, __conf)
    # 5) dump result to disk
    print_to_disk(issues, __resdir)
    # 6) export for Gephi
    print_to_disk_gephi(issues, __resdir)
    # 7) export for jira issue extraction to use them in dev-network-growth
    print_to_disk_extr(issues, __resdir)
    # 8) dump bug issues to disk
    print_to_disk_bugs(issues, __resdir)

    log.info("Jira issue processing complete!")


def load_xml(source_folder):
    """Load issues from disk.

    :param source_folder: the folder where to find .xml-files
    :return: the loaded issue data
    """

    filelist = [f for f in os.listdir(source_folder) if os.path.isfile(os.path.join(source_folder, f))]
    issue_data = list()
    for file in filelist:
        srcfile = os.path.join(source_folder, file)
        log.devinfo("Loading issues from file '{}'...".format(srcfile))

        # check if file exists and exit early if not
        if not os.path.exists(srcfile):
            log.info("Issue file '{}' does not exist! Exiting early...".format(srcfile))
            sys.exit(-1)

        # with open(srcfile, 'r') as issues_file:
        xmldoc = parse(srcfile)
        issue_data.append(xmldoc)

    return issue_data


def merge_user_with_user_from_csv(user, persons):
    """merges list of given users with list of already known users

    :param user: list of users to be merged
    :param persons: list of persons from JIRA (incl. e-mail addresses)
    :return: list of merged users
    """

    new_user = dict()
    if user['username'].lower() in persons.keys():
        new_user['username'] = unicode(user['username'].lower()).encode('utf-8')
        new_user['name'] = unicode(persons.get(user['username'].lower())[0]).encode('utf-8')
        new_user['email'] = unicode(persons.get(user['username'].lower())[1]).encode('utf-8')
    else:
        new_user = user
        log.warning("User not in csv-file: " + str(user))
    log.info("current User: " + str(user) + ",    new user: " + str(new_user))
    return new_user


def parse_xml(issue_data, persons):
    """Parse issues from the xml-data

    :param issue_data: list of xml-files
    :param persons: list of persons from JIRA (incl. e-mail addresses)
    :return: list of parsed issues
    """

    log.info("Parse jira issues...")
    issues = list()
    log.debug("Number of files:" + str(len(issue_data)))
    for issue_file in issue_data:
        issuelist = issue_file.getElementsByTagName('item')
        # re-process all issues
        log.debug("Number of issues:" + str(len(issuelist)))
        for issue_x in issuelist:
            # temporary container for references
            comments = list()
            issue = dict()
            components = []
            refs = []

            # parse values form xml
            # add issue values to the issue
            key = issue_x.getElementsByTagName('key')[0]
            issue['id'] = key.attributes['id'].value
            issue['externalId'] = key.firstChild.data

            created = issue_x.getElementsByTagName('created')[0]
            createDate = created.firstChild.data
            d = datetime.strptime(createDate, '%a, %d %b %Y %H:%M:%S +0000')
            issue['creationDate'] = d.strftime('%Y-%m-%d %H:%M:%S')

            resolved = issue_x.getElementsByTagName('resolved')
            issue['resolveDate'] = ""
            if (len(resolved) > 0) and (not resolved[0] is None):
                resolveDate = resolved[0].firstChild.data
                d = datetime.strptime(resolveDate, '%a, %d %b %Y %H:%M:%S +0000')
                issue['resolveDate'] = d.strftime('%Y-%m-%d %H:%M:%S')

            link = issue_x.getElementsByTagName('link')[0]
            issue['url'] = link.firstChild.data

            type = issue_x.getElementsByTagName('type')[0]
            issue['type'] = type.firstChild.data

            status = issue_x.getElementsByTagName('status')[0]
            issue['state'] = status.firstChild.data

            project = issue_x.getElementsByTagName('project')[0]
            issue['projectId'] = project.attributes['id'].value

            resolution = issue_x.getElementsByTagName('resolution')[0]
            issue['resolution'] = resolution.firstChild.data

            for component in issue_x.getElementsByTagName('component'):
                components.append(component.firstChild.data)
            issue['components'] = components

            for ref in issue_x.getElementsByTagName('issuelinktype'):
                refId = ref.getElementsByTagName('issuekey')[0].firstChild.data
                refType = ref.getElementsByTagName('name')[0].firstChild.data
                refs.append((refId, refType))
            issue['references'] = refs

            reporter = issue_x.getElementsByTagName('reporter')[0]
            user = dict()
            user["username"] = reporter.attributes['username'].value
            user["name"] = reporter.firstChild.data
            user["email"] = ""
            issue["author"] = merge_user_with_user_from_csv(user, persons)

            issue['title'] = issue_x.getElementsByTagName('title')[0].firstChild.data

            # add comments / issue_changes to the issue
            for comment_x in issue_x.getElementsByTagName('comment'):
                comment = dict()
                comment['id'] = comment_x.attributes['id'].value
                user = dict()
                user["username"] = comment_x.attributes['author'].value
                user["name"] = ""
                user["email"] = ""
                comment["author"] = merge_user_with_user_from_csv(user, persons)
                comment['state'] = issue['state']
                comment['resolution'] = issue['resolution']

                created = comment_x.attributes['created'].value
                d = datetime.strptime(created, '%a, %d %b %Y %H:%M:%S +0000')
                comment['changeDate'] = d.strftime('%Y-%m-%d %H:%M:%S')

                comment['text'] = comment_x.firstChild.data
                comment['issueId'] = issue['id']
                comments.append(comment)

            issue['comments'] = comments

            # add relations to the issue
            relations = list()
            for rel in issue_x.getElementsByTagName('issuelinktype'):
                relation = dict()
                relation['relation'] = rel.getElementsByTagName('name')[0].firstChild.data

                if (rel.hasAttribute('inwardlinks')):
                    left = rel.getElementsByTagName('inwardlinks')
                    issuekeys = left.getElementsByTagName("issuekey")
                    for key in issuekeys:
                        relation['type'] = "inward"
                        relation['id'] = key.firstChild.data
                        relations.append(relation)

                if (rel.hasAttribute('outwardlinks')):
                    right = rel.getElementsByTagName('outwardlinks')
                    issuekeys = right.getElementsByTagName('issuekey')
                    for key in issuekeys:
                        relation['type'] = "outward"
                        relation['id'] = key.firstChild.data
                        relations.append(relation)

            issue["relations"] = relations
            issues.append(issue)
    log.debug("number of issues after parse_xml: '{}'".format(len(issues)))
    return issues


def load_issue_via_api(issues, persons):
    """For each issue in the list the history is added via the api

        :param issues: list of issues
        :param persons: list of persons from JIRA (incl. e-mail addresses)
    """""

    log.info("Load issue information via api...")
    jira_project = JIRA(project_url)

    for issue in issues:

        min_date = issue['creationDate']
        api_issue = jira_project.issue(issue['externalId'], expand='changelog')
        changelog = api_issue.changelog
        histories = list()

        for change in changelog.histories:

            old_state, new_state, old_resolution, new_resolution = "None", "None", "None", "None"

            for item in change.items:

                if item.field == 'status':
                    old_state = str(item.fromString)
                    new_state = str(item.toString)

                elif item.field == 'resolution':
                    old_resolution = str(item.fromString)
                    new_resolution = str(item.toString)

            # only history entries that change the state or the resolution are interesting
            if (old_state != new_state) | (old_resolution != new_resolution):

                history = dict()
                history['state'] = new_state
                history['resolution'] = new_resolution
                user = dict()
                user["username"] = change.author.name
                user["name"] = change.author.name
                user["email"] = ""
                history["author"] = merge_user_with_user_from_csv(user, persons)
                date = change.created
                d = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f+0000")
                history['date'] = d.strftime('%Y-%m-%d %H:%M:%S')
                histories.append(history)

                # updates state and resolution for issue and comments in the past
                if (min_date <= issue['creationDate']) & (issue['creationDate'] < history['date']):
                    issue['state'] = old_state
                    issue['resolution'] = old_resolution

                for comment in issue['comments']:

                    if (min_date <= comment['changeDate']) & (comment['changeDate'] < history['date']):
                        comment['state'] = old_state
                        comment['resolution'] = old_resolution

                min_date = history['date']

        issue['history'] = histories


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
        mail = unicode(user["email"]).encode("utf-8")  # empty
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
        issue["author"] = get_or_update_user(issue["author"])

        # check database for event authors
        for comment in issue["comments"]:
            # get the event user from the DB
            comment["author"] = get_or_update_user(comment["author"])
            ## get the reference-target user from the DB if needed
            # if event["ref_target"] != "":
            #   event["ref_target"] = get_or_update_user(event["ref_target"])

    log.debug("number of issues after insert_user_data: '{}'".format(len(issues)))
    return issues


def print_to_disk(issues, results_folder):
    """Print issues to file 'issues-jira.list' in result folder

    :param issues: the issues to dump
    :param results_folder: the folder where to place 'issues-jira.list' output file
    """

    # construct path to output file
    output_file = os.path.join(results_folder, "issues-jira.list")
    log.info("Dumping output in file '{}'...".format(output_file))

    # construct lines of output
    lines = []
    for issue in issues:
        log.info("Current issue '{}'".format(issue['externalId']))
        lines.append((issue["author"]['name'],
                      issue["author"]['email'],
                      issue['externalId'],
                      issue['creationDate'],
                      issue['externalId'],
                      issue['type']))
        for comment in issue["comments"]:
            lines.append((
                comment['author']['name'],
                comment['author']['email'],
                comment["id"],
                comment['changeDate'],
                issue['externalId'],
                "comment"
            ))
    # write to output file
    csv_writer.write_to_csv(output_file, lines)


def print_to_disk_bugs(issues, results_folder):
    """Sorts of bug issues and prints them to file 'bugs-jira.list' in result folder

    :param issues: the issues to sort of bugs
    :param results_folder: the folder where to place 'bugs-jira.list' output file
    """

    # construct path to output file
    output_file = os.path.join(results_folder, "bugs-jira.list")
    log.info("Dumping output in file '{}'...".format(output_file))

    # construct lines of output
    lines = []
    for issue in issues:
        log.info("Current issue '{}'".format(issue['externalId']))

        # only writes issues with type bug and their comments in the output file
        if issue['type'] == "Bug":
            lines.append((
                issue['externalId'],
                issue['state'],
                issue['creationDate'],
                issue['resolveDate'],
                False,  ## Value of is.pull.request
                issue['author']['name'],
                issue['author']['email'],
                issue['creationDate'],
                issue['references'],
                "open",  ## event.name
                issue['resolution'],
                issue['components']
            ))

            lines.append((
                issue['externalId'],
                issue['state'],
                issue['creationDate'],
                issue['resolveDate'],
                False,  ## Value of is.pull.request
                issue['author']['name'],
                issue['author']['email'],
                issue['creationDate'],
                "",  ## ref.name
                "commented",  ## event.name
                issue['resolution'],
                "",  ##components
            ))

            for comment in issue["comments"]:
                lines.append((
                    issue['externalId'],
                    comment['state'],
                    issue['creationDate'],
                    issue['resolveDate'],
                    False,  ## Value of is.pull.request
                    comment['author']['name'],
                    comment['author']['email'],
                    comment['changeDate'],
                    "",  ## ref.name
                    "commented",  ## event.name
                    comment['resolution'],
                    "",  ##components
                ))

            if with_api:
                for history in issue['history']:
                    lines.append((
                        issue['externalId'],
                        history['state'],
                        issue['creationDate'],
                        issue['resolveDate'],
                        False,  ## Value of is.pull.request
                        history['author']['name'],
                        history['author']['email'],
                        history['date'],
                        "",  ## ref.name
                        "updated",  ## event.name
                        history['resolution'],
                        ""  ##components
                    ))

    # write to output file
    csv_writer.write_to_csv(output_file, lines)


def print_to_disk_extr(issues, results_folder):
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
        log.info("Current issue '{}'".format(issue['externalId']))

        lines.append((
            issue['externalId'],
            issue['state'],
            issue['creationDate'],
            issue['resolveDate'],
            False,  ## Value of is.pull.request
            issue['author']['name'],
            issue['author']['email'],
            issue['creationDate'],
            "",  ## ref.name
            "open"  ## event.name
        ))

        lines.append((
            issue['externalId'],
            issue['state'],
            issue['creationDate'],
            issue['resolveDate'],
            False,  ## Value of is.pull.request
            issue['author']['name'],
            issue['author']['email'],
            issue['creationDate'],
            "",  ## ref.name
            "commented"  ## event.name
        ))

        for comment in issue["comments"]:
            lines.append((
                issue['externalId'],
                issue['state'],
                issue['creationDate'],
                issue['resolveDate'],
                False,  ## Value of is.pull.request
                comment['author']['name'],
                comment['author']['email'],
                comment['changeDate'],
                "",  ## ref.name
                "commented"  ## event.name
            ))
    # write to output file
    csv_writer.write_to_csv(output_file, lines)


def print_to_disk_gephi(issues, results_folder):
    """Print issues to file 'issues-jira-gephi-nodes.csv' and
    'issues-jira-gephi-edges.csv' in result folder. The files can be
     used to build dynamic networks in Gephi.

    :param issues: the issues to dump
    :param results_folder: the folder where to place the two output file
    """

    # construct path to output file
    output_file_nodes = os.path.join(results_folder, "issues-jira-gephi-nodes.csv")
    output_file_edges = os.path.join(results_folder, "issues-jira-gephi-edges.csv")
    log.info("Dumping output in file '{}'...".format(output_file_nodes))
    log.info("Dumping output in file '{}'...".format(output_file_edges))

    # construct lines of output
    node_lines = []
    edge_lines = []
    node_lines.append(("Id", "Type"))
    edge_lines.append(("Source", "Target", "Timestamp", "Edgetype"))
    for issue in issues:
        node_lines.append((issue['externalId'], "Issue"))
        node_lines.append((issue["author"]['name'], "Person"))

        edge_lines.append((issue["author"]['name'], issue['externalId'], issue['creationDate'], "Person-Issue"))
        for comment in issue["comments"]:
            node_lines.append((comment['id'], "Comment"))
            node_lines.append((comment['author']['name'], "Person"))

            edge_lines.append((issue['externalId'], comment['id'], comment['changeDate'],
                               "Issue-Comment"))
            edge_lines.append((comment['author']['name'], comment['id'], ['changeDate'],
                               "Person-Comment"))
    # write to output file
    csv_writer.write_to_csv(output_file_edges, edge_lines)
    csv_writer.write_to_csv(output_file_nodes, node_lines)


def load_csv(source_folder):
    """Load persons from disk.

    :param source_folder: the folder where to find .csv-file
    :return: the loaded person data
    """

    def find_first_existing(source_folder, filenames):
        """
        Check if any of the given file names exist in the given folder and return the first existing.

        :param source_folder: the folder where to search for the given file names
        :param filenames: the file names to search for
        :return: the first existing file name, None otherwise
        """

        filenames = map(lambda fi: os.path.join(source_folder, fi), filenames)
        existing = map(lambda fi: os.path.exists(fi), filenames)
        first = next((i for (i, x) in enumerate(existing) if x), None)

        if first is not None:
            return filenames[first]
        else:
            return None

    person_files = (
        "jira-comment-authors-with-email.csv",
        "jira_issue_comments.csv"
    )
    srcfile = find_first_existing(source_folder, person_files)

    # check if file exists and exit early if not
    if not srcfile:
        log.error("Person files '{}' do not exist! Exiting early...".format(person_files))
        sys.exit(-1)

    log.devinfo("Loading person csv from file '{}'...".format(srcfile))
    with open(srcfile, 'r') as f:
        person_data = csv.DictReader(f, delimiter=',', skipinitialspace=True)
        persons = {}
        for row in person_data:
            if not row['AuthorID'] in persons.keys():
                persons[row['AuthorID']] = (row['AuthorName'], row['userEmail'])

    return persons
