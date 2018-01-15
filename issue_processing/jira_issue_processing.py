import argparse
import httplib
import json
import os
import sys
import urllib
import time
import csv

from os import listdir
from os.path import isfile, join
from xml.dom.minidom import parse, parseString
from datetime import datetime

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
    folder = args.resdir
    __srcdir = os.path.abspath(os.path.join(folder, __conf['repo'] + "_proximity", "conway", "issues_xml"))
    __resdir = os.path.abspath(os.path.join(args.resdir, __conf['project'], __conf["tagging"]))

	# get person folder
	#__psrcdir = os.path.abspath(os.path.join(folder, __conf['repo'] + "_proximity", "conway"))

    # run processing of issue data:
	#yxz = load_csv(__psrcdir)
    #persons = parse_csv(yxz)
	
    # 1) load the list of issues
    issues = load(__srcdir)
    # 2) re-format the issues
    issues = parse_xml(issues)
    # 3) update user data with Codeface database
    issues = insert_user_data(issues, __conf)
    # 4) dump result to disk
    print_to_disk(issues, __resdir)
    # 5) export for Gephi
    print_to_disk_gephi(issues, __resdir)
    # 6) export for github issue extraction
    print_to_disk_extr(issues, __resdir)

    log.info("Issue processing complete!")


def load(source_folder):
    """Load issues from disk.

    :param source_folder: the folder where to find .xml-files
    :return: the loaded issue data
    """
    filelist =  [f for f in os.listdir(source_folder) if isfile(join(source_folder, f))]
    issue_data = list()
    for file in filelist:
        srcfile = os.path.join(source_folder, file)
        log.devinfo("Loading issues from file '{}'...".format(srcfile))

        # check if file exists and exit early if not
        if not os.path.exists(srcfile):
            log.info("Issue file '{}' does not exist! Exiting early...".format(srcfile))
            sys.exit(-1)

        #with open(srcfile, 'r') as issues_file:
        xmldoc = parse(srcfile)
        issue_data.append(xmldoc)

    return issue_data


def load_csv(source_folder):
    """Load persons from disk.

    :param source_folder: the folder where to find .csv-file
    :return: the loaded person data
    """

    srcfile = os.path.join(source_folder, "jira-comment-authors-with-email.csv")
    log.devinfo("Loading person csv from file '{}'...".format(srcfile))

     # check if file exists and exit early if not
    if not os.path.exists(srcfile):
        log.info("Person file '{}' does not exist! Exiting early...".format(srcfile))
        sys.exit(-1)


    person_data = list()
     #with open(srcfile, 'r') as person_file:
    with open(srcfile, 'r') as f:
         person_data = csv.DictReader(f, delimiter=',', skipinitialspace=True)

    return person_data


def parse_csv(person_date):
    for row in person_date:
        print(row['AuthorID'], row['userEmail'])

	return persons

def parse_xml(issue_data):
    """Parse issues from the xml-data

        :param issue_data: xml-data
        :return: the loaded issue data
        """

    log.devinfo("Parse issues...")
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

            #parse values form xml
            #add issue values to the issue
            key = issue_x.getElementsByTagName('key')[0]
            issue['id'] = key.attributes['id'].value
            issue['externalId'] = key.firstChild.data

            created = issue_x.getElementsByTagName('created')[0]
            createDate = created.firstChild.data
            issue['creationDate'] = datetime.strptime(createDate, '%a, %d %b %Y %H:%M:%S +0000')

            resolved = issue_x.getElementsByTagName('resolved')
            issue['resolveDate'] = ""
            if (len(resolved) > 0) and (not resolved[0] is None) :
                resolveDate = resolved[0].firstChild.data
                issue['resolveDate'] = datetime.strptime(resolveDate, '%a, %d %b %Y %H:%M:%S +0000')

            link = issue_x.getElementsByTagName('link')[0]
            issue['url'] = link.firstChild.data

            type = issue_x.getElementsByTagName('type')[0]
            issue['type'] = type.firstChild.data

            status = issue_x.getElementsByTagName('status')[0]
            issue['state'] = status.firstChild.data

            project = issue_x.getElementsByTagName('project')[0]
            issue['projectId'] = project.attributes['id'].value

            assignee = issue_x.getElementsByTagName('reporter')[0]
            user = dict()
            user["username"] = assignee.attributes['username'].value
            user["name"] = assignee.attributes['username'].value	##assignee.firstChild.data
            user["email"] = ""
            issue["user"] = user

            issue['title'] = issue_x.getElementsByTagName('title')[0].firstChild.data

            # add comments / issue_changes to the issue
            for comment_x in issue_x.getElementsByTagName('comment'):
                comment = dict()
                comment['id'] = comment_x.attributes['id'].value
                user = dict()
                user["username"] = comment_x.attributes['author'].value
                user["name"] = comment_x.attributes['author'].value   #not correct, it is the username
                user["email"] = ""
                comment["author"] = user

                created = comment_x.attributes['created'].value
                comment['changeDate'] = datetime.strptime(created, '%a, %d %b %Y %H:%M:%S +0000')

                comment['text'] = comment_x.firstChild.data
                comment['issueId'] = issue['id']
                comments.append(comment)

            issue['comments'] = comments

            # add relations to the issue
            relations = list()
            for rel in issue_x.getElementsByTagName('issuelinktype'):
                relation = dict()
                relation['relation'] = rel.getElementsByTagName('name')[0].firstChild.data

                #left = rel.getElementsByTagName('inwardlinks')
                #right = rel.getElementsByTagName('outwardlinks')

                #if (not left is None):
                #    issuekey = left.getElementsByTagName('issuekey')
                #    if (not issuekey is None):
                #        relation['leftIssueId'] = issuekey[0].attributes['id'].value

                #    relation['rightIssueId'] = issue['id']

                #if (not right is None):
                #    issuekey = right.getElementsByTagName('issuekey')
                #    if (not issuekey is None):
                #        relation['rightIssueId'] = issuekey[0].attributes['id'].value

                 #   relation['leftIssueId'] = issue["id"]

                #TODO : relation['id']
                relations.append(relation)

            issue["relations"] = relations
            issues.append(issue)
    log.debug("number of issues after parse_xml: '{}'".format(len(issues)))
    return issues


def insert_user_data(issues, conf):
    """Insert user data into database ad update issue data.

    :param issues: the issues to retrieve user data from
    :param conf: the project configuration
    :return: the updated issue data
    """

    log.info("Syncing users with ID service...")

	## auslesen aus csv-file

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
        mail = unicode(user["email"]).encode("utf-8")      #empty
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
        for comment in issue["comments"]:
            # get the event user from the DB
            comment["author"] = get_or_update_user(comment["author"])
            ## get the reference-target user from the DB if needed
            #if event["ref_target"] != "":
             #   event["ref_target"] = get_or_update_user(event["ref_target"])

    log.debug("number of issues after insert_user_data: '{}'".format(len(issues)))
    return issues


def print_to_disk(issues, results_folder):
    """Print issues to file 'issues.list' in result folder

    :param issues: the issues to dump
    :param results_folder: the folder where to place 'issues.list' output file
    """

    # construct path to output file
    output_file = os.path.join(results_folder, "jira-issues.list")
    log.info("Dumping output in file '{}'...".format(output_file))

    # construct lines of output
    lines = []
    for issue in issues:
        log.info("Current issue '{}'".format(issue['externalId']))
        lines.append((issue['user']['username'],
                      issue['user']['email'],
                      issue['externalId'],
                      issue['creationDate'],
                      issue['externalId'],
                      issue['type']))
        for comment in issue["comments"]:
            lines.append((
                comment['author']['username'],
                comment['author']['email'],
                comment["id"],
                comment['changeDate'],
                issue['externalId'],
                "comment"
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
        for comment in issue["comments"]:
            lines.append((
                issue['externalId'],
                issue['state'],
                issue['creationDate'],
                issue['resolveDate'],
                False,  ## Value of is.pull.request
                comment['author']['username'],
                comment['author']['email'],
                comment['changeDate'],
                "",                             ## ref.name
                "commented"                     ## event.name
            ))

    # write to output file
    csv_writer.write_to_csv(output_file, lines)


def print_to_disk_gephi(issues, results_folder):
    """Print issues to file 'issues.list' in result folder

    :param issues: the issues to dump
    :param results_folder: the folder where to place 'issues.list' output file
    """

    # construct path to output file
    output_file_nodes = os.path.join(results_folder, "issue_nodes.csv")
    output_file_edges = os.path.join(results_folder, "issue_edges.csv")
    log.info("Dumping output in file '{}'...".format(output_file_nodes))
    log.info("Dumping output in file '{}'...".format(output_file_edges))

    # construct lines of output
    node_lines = []
    edge_lines = []
    node_lines.append(("Id", "Type"))
    edge_lines.append(("Source", "Target", "Timestamp", "Edgetype"))
    for issue in issues:
        node_lines.append((issue['externalId'], "Issue"))
        node_lines.append((issue['user']['username'], "Person"))

        edge_lines.append((issue['user']['username'], issue['externalId'], time.mktime(issue['creationDate'].timetuple()), "Person-Issue"))
        for comment in issue["comments"]:
            node_lines.append((comment['id'], "Comment"))
            node_lines.append((comment['author']['username'], "Person"))

            edge_lines.append((issue['externalId'], comment['id'], time.mktime(comment['changeDate'].timetuple()),
                               "Issue-Comment"))
            edge_lines.append((comment['author']['username'], comment['id'], time.mktime(comment['changeDate'].timetuple()),
                               "Person-Comment"))

    # write to output file
    csv_writer.write_to_csv(output_file_edges, edge_lines)
    csv_writer.write_to_csv(output_file_nodes, node_lines)
