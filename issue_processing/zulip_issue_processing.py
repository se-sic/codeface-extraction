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
# Copyright 2018-2019 by Anselm Fehnker <fehnker@fim.uni-passau.de>
# Copyright 2019 by Thomas Bock <bockthom@fim.uni-passau.de>
# Copyright 2020-2021 by Thomas Bock <bockthom@cs.uni-saarland.de>
# Copyright 2026 by Thomas Bock <bockthom@cmu.edu>
# Copyright 2025 by Maximilian Löffler <s8maloef@stud.uni-saarland.de>
# Copyright 2025-2026 by Ritika Hiremath <rihi00002@stud.uni-saarland.de>
# All Rights Reserved.
"""
This file is able to extract Zulip issue data from json files.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from logging import getLogger

from codeface_utils.cluster.idManager import dbIdManager, csvIdManager
from codeface_utils.configuration import Configuration
from codeface_utils.dbmanager import DBManager
from codeface_utils.util import setup_logging
from dateutil import parser as dateparser
from bs4 import BeautifulSoup

from csv_writer import csv_writer

# create logger
setup_logging()
log = getLogger(__name__)

# datetime format string
datetime_format = "%Y-%m-%d %H:%M:%S"

def run():
    # get all needed paths and arguments for the method call.
    parser = argparse.ArgumentParser(prog='codeface-extraction-issues-github', description='Codeface extraction')
    parser.add_argument('-c', '--config', help="Codeface configuration file", default='codeface.conf')
    parser.add_argument('-p', '--project', help="Project configuration file", required=True)
    parser.add_argument('resdir', help="Directory to store analysis results in")

    # parse arguments
    args = parser.parse_args(sys.argv[1:])
    __codeface_conf, __project_conf = list(map(os.path.abspath, (args.config, args.project)))

    # create configuration
    __conf = Configuration.load(__codeface_conf, __project_conf)

    # get source and results folders
    __srcdir = os.path.abspath(os.path.join(args.resdir, __conf['repo'] + "_issues"))
    __resdir = os.path.abspath(os.path.join(args.resdir, __conf['project'], __conf["tagging"]))
    __userdir = os.path.abspath(os.path.join(args.resdir, __conf['project'], __conf["tagging"]))
    # run processing of issue data:
    # 1) load the list of issues
    issues = load(__srcdir)
    log.info("Source file loaded")
    users = load_users(__userdir)
    # 2) update missing columns
    issues = update(issues, users)
    # 3) re-format the issues
    issues = reformat_issues(issues)
    # 5) update user data with Codeface database and dump username-to-name/e-mail list
    issues = insert_user_data(issues, __conf, __resdir)
    # 6) dump result to disk
    print_to_disk(issues, __resdir)
    log.info("Zulip issue processing complete!")


def load(source_folder):
    """Load issues from disk.

    :param source_folder: the folder where to find 'zulip.json'
    :return: the loaded zulip data
    """

    srcfile = os.path.join(source_folder, "zulip.json")
    log.info("Loading Zulip data from file '{}'...".format(srcfile))

    # check if file exists and exit early if not
    if not os.path.exists(srcfile):
        log.error("Zulip data file '{}' does not exist! Exiting early...".format(srcfile))
        sys.exit(-1)

    with open(srcfile) as issues_file:
        issue_data = json.load(issues_file)

    return issue_data

def load_users(source_folder):
    """Load users list from disk if it exists.

    :param source_folder: the folder where to find 'usernames.list'
    :return: the loaded zulip data
    """

    srcfile = os.path.join(source_folder, "usernames.list")
    log.info("Loading users data from file '{}'...".format(srcfile))

    # check if file exists and exit early if not
    if not os.path.exists(srcfile):
        log.error("Users data file '{}' does not exist! Continuing without...".format(srcfile))
        return {}
    
    users = {}
    # cleans and opens the source file
    with open(srcfile, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")
            if len(parts) != 3:
                log.warning(f"Skipping malformed line: {line}")
                continue

            username, name, email = [p.strip('"') for p in parts]

            # creates access to users data through username unless it is None
            if username != "None":
                users[username] = {"name": name, "email": email}

            # creates access to users data through name 
            users[name] = {
                "username": "" if username == "None" else username,
                "email": email,
            }

    log.info(f"Loaded {len(users)} users")
    return users

def format_time(time):
    """
    Format times from different sources to a consistent time format

    :param time: the time that shall be formatted
    :return: the formatted time
    """

    # empty time would be formatted to current date
    if time == "" or time is None:
        return ""
    else:
        d = datetime.fromtimestamp(time)
        return d.strftime(datetime_format)


def subtract_seconds_from_time(time, seconds):
    """
    Subtract the specified number of seconds from a date string

    :param time: the date string to subtract the specified seconds from
    :param seconds: the number of seconds to subtract from the date string
    :return: the date string after subtracting the specified number of seconds
    """

    new_time = datetime.strptime(time, datetime_format) - timedelta(seconds = seconds)
    return new_time.strftime(datetime_format)


def create_user(name, username, email):
    """
    Creates a user object with all needed information

    :param name: the name the user shall have
    :param username: the username the user shall have
    :param email:  the email the user shall have
    :return: the created user object
    """

    if name is None:
        name = ""
    if username is None:
        username = ""
    if email is None:
        email = ""

    user = dict()
    user["username"] = username
    user["name"] = name
    user["email"] = email

    return user


def create_deleted_user():
    """
    Creates a user object for a deleted user (ghost user)

    :return: the created user object for a deleted user
    """

    return create_user("Deleted user", "ghost", "ghost@github.com")


def lookup_user(user_dict, user):
    """
    Alters a user object in the case that name or email are missing by the corresponding name and email
    from a given user dictionary

    :param user_dict: the user dictionary
    :param user: the user object to lookup in the dictionary
    :return: the altered user object in case of a lookup
             or the unaltered user object otherwise
    """

    # if user is None, replace it by a deleted user
    if user is None:
        user = create_deleted_user()

    if (user["name"] == "" or user["name"] is None or
        user["email"] is None or user["email"] == ""):

        # lookup user only if username is not None and not empty
        if user["username"] is not None and not user["username"] == "":
            user = user_dict[user["username"]]

    return user

def update_user_dict(user_dict, user):
    """
    Adds or updates users to merge GitHub usernames and names and e-mail addresses originating from the git repository

    :param user_dict: the user dictionary
    :param user: the user object to add to or update in the dictionary
    :return: the updated user dictionary
    """

    # if the given user is None, use the deleted user instead
    if user is None:
        user = create_deleted_user()

    if user["username"] not in list(user_dict.keys()):
        if user["username"] is not None and not user["username"] == "":
            user_dict[user["username"]] = user
    else:
        user_in_dict = user_dict[user["username"]]
        if user_in_dict["name"] is None or user_in_dict["name"] == "":
            user_in_dict["name"] = user["name"]
        if user_in_dict["email"] is None or user_in_dict["email"] == "":
            user_in_dict["email"] = user["email"]
        user_dict[user["username"]] = user_in_dict

    return user_dict

def discussion_id_update(issue_data):
    """
    Updates the id for each issue data in zulip. 
    The ID is dependent on the topic id and followed by # and
      the count of the message from the beginning.
    :param issue_data: total issue data from zulip
    : return: The updated issue data with the id updated.
    """
    grouped = {}
    # groups each discussion topic together to update the id
    for item in issue_data:
        topic = item["discussion_topic"]
        if not topic:
            log.warning("Missing discussion_topic in item:" + str(item))
            continue
        if topic not in grouped:
            grouped[topic] = []
            log.debug("New topic group created:" + str(topic))
        grouped[topic].append(item)

    # Updates the discussion id here. 
    for topic, messages in grouped.items():
        messages.sort(key=lambda m: m["timestamp"])
        for idx, msg in enumerate(messages, start=1):
            msg["discussion_id"] = f'{msg["discussion_id"]}#{idx}'

    log.info("Finished updating discussion id for all issue data")
    return issue_data

def discussion_begin_end_add(issue_data):
    """
    Updates the discussion begin and end time for each discussion topic.
    :param issue_data: Total issue data 
    :return: the updated issue data with two new columns.
    """
    
    discussion_topics = {}
    # groups discussion topics and finds the max and min time that topic was discussed.
    for item in issue_data:
        d_topic = item["discussion_topic"]
        ts = item["timestamp"]

        if d_topic not in discussion_topics:
            discussion_topics[d_topic] = []
            log.debug("New discussion topic found:" + str(d_topic))
        discussion_topics[d_topic].append(ts)

    if not discussion_topics:
        log.warning("No discussion topics were grouped!")

    discussion_bounds = {
        d_topic: {
            "discussion_begin": min(times),
            "discussion_end": max(times)
        }
        for d_topic, times in discussion_topics.items()
    }

    # for d_topic, bounds in discussion_bounds.items():
    #     log.debug("Topic" + str(d_topic) + "->" + str(bounds))

    for item in issue_data:
        d_topic = item["discussion_topic"]
        item["discussion_begin"] = discussion_bounds[d_topic]["discussion_begin"]
        item["discussion_end"] = discussion_bounds[d_topic]["discussion_end"]
        log.debug("Discussion bounds updated for "+ str(item))

    log.info("Finished updating discussion bounds")
    return issue_data

def bot_event_type(issue):
    """
    Updates the type of event . 
    This function only updates events notification bot username "Notification Bot".
    
    :param issue: the zulip issue data to update 
    :return: the event type
    """
    # extracts the content into a variable
    content = issue.get("content", "").lower()
    # checks if the event type is present in the string
    if "stream created" in content:
        return "stream created"

    if "changed the description" in content:
        return "stream description changed"
    
    if "changed the access permissions" in content:
        return "stream permissions changed"
    
    if "renamed stream" in content:
        return "stream renamed"

    if "wave" in content:
        return "wave"

    return "unclassified event"

def notification_bot_event(issue):
    """
    Updates the type of event . 
    This function only updates events notification bot does with no change in username.
    
    :param issue: the zulip issue data to update 
    :return: the event type
    """
    # extracts the content into a variable
    content = issue.get("content", "").lower()
    # checks if the event type is present in the string
    if "has marked this topic as resolved" in content:
        return "topic resolved"

    if "has marked this topic as unresolved" in content:
        return "topic unresolved"
    
    if "topic was moved" in content:
        return "topic moved"
    
    return "user event"

def bot_event_name_update(issue):
    """
    For events from notification bot.
    It finds the user in content string and returns it  
    
    :param issue: a single zulip-issue data to find the user name for.
    :return: returns the user name
    """
    soup = BeautifulSoup(issue["content"], "html.parser")
    mention = soup.find("span", class_="user-mention")

    if mention:
        return mention.get_text(strip=True)

    return None

def create_update_user(issue, users):
    """
    Creates user for each issue data. 
    Classifies name and username based on whethre the name has a space or not.
    
    :param issue: A single issue data from the zulip issue data
    :reutrn: returns a dictionary to update issue["user"]
    """
    log.debug("Creating user for issue " + str(issue["discussion_id"]))

    dict_issue = {}
    sender = issue["sender_full_name"]

    # check if present in users list 
    if sender in users:
        info = users[sender]

        dict_issue["username"] = info.get("username", sender)
        dict_issue["name"] = info.get("name", sender)
        dict_issue["email"] = info.get("email", issue["sender_email"])

    else:
        # fallback
        # checks if there is a space in the string sender. 
        if " " in sender:
            dict_issue["username"] = ""
            dict_issue["name"] = sender
            
        # if it does not find a name then updates both with sa
        else:
            dict_issue["username"] = sender
            dict_issue["name"] = sender

        dict_issue["email"] = issue["sender_email"]

    # log.debug("New User dict created for" + str(issue["discussion_topic"]) + "with" + str(issue["discussion_id"]))
    return dict_issue
        
def event_type_and_user(issue_data, username):
    """
    Checks if the event is a stream events. 
    updates the event type and sender details, if sender name is made into notification bot.
    :param issue_data: total zulip issue data to check and update the event types.
    :return: returns the updated zulip issue data
    """

    for issue in issue_data:
        if(("stream events" in issue["discussion_topic"]) and (issue["sender_full_name"] == "Notification Bot")):
            # log.debug("Bot stream event detected")
            # updates name and discussion 
            issue["individual_events"]= bot_event_type(issue)
            issue["sender_full_name"] = bot_event_name_update(issue)
            soup = BeautifulSoup(issue["content"], "html.parser")
            span_data = soup.find("span", class_="user-mention") 
            if span_data:
                user_id = span_data.get("data-user-id")
            issue["sender_email"] = user_id+"@zulipchat.com"

        else:
            if("stream events" in issue["discussion_topic"]):
                # log.debug("User stream event detected")
                # updates the event type when issue name is proper user name.
                issue["individual_events"] = notification_bot_event(issue)
            
            issue["individual_events"]= "commented event"
        # creates user for each issue data. Combines name, email and username into a dictionary.
        issue["user"] = create_update_user(issue, username)

    log.info("Finished updating event types and users")
    return issue_data

def update(issue_data, users):
    """
    updates values in the issue data as per requirement.
    :params: issue_data: the issue data to be updated.
    :params: usernames: existing list of user data
    :return: returns the issue data.
    """

    # sends the entirety of the issue data to update discussion id, discussion begin, discussion end, event type, and user.
    issue_data = discussion_id_update(issue_data)
    issue_data = discussion_begin_end_add(issue_data)
    issue_data = event_type_and_user(issue_data,users)

    log.info("Issue data updated ...")
    return issue_data

def reformat_issues(issue_data):
    """
    Re-arrange issue data structure.

    :param issue_data: the issue data to re-arrange
    :return: the re-arranged issue data
    """

    log.info("Re-arranging Github issues...")

    # re-process all issues
    for issue in issue_data:

        # empty container for issue types
        issue["type"] = []

        # empty container for issue resolutions
        issue["resolution"] = []

        issue["discussion_begin"] = format_time(issue["discussion_begin"])

        # parses the close time in the correct format
        issue["discussion_end"] = format_time(issue["discussion_end"])

        issue["timestamp"] = format_time(issue["timestamp"])

        issue["type"].append("topic")

    return issue_data

def insert_user_data(issues, conf, resdir):
    """
    Insert user data into database and update issue data.
    In addition, dump username-to-user list to file.

    :param issues: the issues to retrieve user data from
    :param conf: the project configuration
    :param resdir: the directory in which the username-to-user-list should be dumped
    :return: the updated issue data
    """

    log.info("Syncing users with ID service...")

    # create buffer for users (key: user id)
    user_buffer = dict()
    # create buffer for user ids (key: user string)
    user_id_buffer = dict()
    # create buffer for usernames (key: username)
    username_id_buffer = dict()

    # connect to ID service
    if conf["useCsv"]:
        idservice = csvIdManager(conf)
    else:
        dbm = DBManager(conf)
        idservice = dbIdManager(dbm, conf)

    def get_user_string(name, email):
        if not email or email is None:
            return "{name}".format(name=name)
            # return "{name} <{name}@default.com>".format(name=name)  # for debugging only
        else:
            return "{name} <{email}>".format(name=name, email=email)

    def get_id_and_update_user(user, buffer_db_ids=user_id_buffer, buffer_usernames=username_id_buffer):

        # ensure string representation for name and e-mail address
        username = str(user["username"])
        name = str(user["name"]) if user["name"] is not None else username
        mail = str(user["email"])

        # construct string for ID service and send query
        user_string = get_user_string(name, mail)

        # check buffer to reduce amount of DB queries
        if user_string in buffer_db_ids:
            log.info("Returning person id for user '{}' from buffer.".format(user_string))
            if username is not None:
                buffer_usernames[username] = buffer_db_ids[user_string]
            return buffer_db_ids[user_string]

        # get person information from ID service
        log.info("Passing user '{}' to ID service.".format(user_string))
        idx = idservice.getPersonID(user_string)

        # add user information to buffer
        # user_string = get_user_string(user["name"], user["email"]) # update for
        buffer_db_ids[user_string] = idx

        # add id to username buffer
        if username is not None:
            buffer_usernames[username] = idx

        return idx

    def get_user_from_id(idx, buffer_db=user_buffer):

        # check whether user information is in buffer to reduce amount of DB queries
        if idx in buffer_db:
            log.info("Returning user '{}' from buffer.".format(idx))
            return buffer_db[idx]

        # get person information from ID service
        log.info("Passing user id '{}' to ID service.".format(idx))
        person = idservice.getPersonFromDB(idx)
        user = {
            "name": person["name"],
            "email": person["email1"],
            "id": person["id"]
        }

        # add user information to buffer
        buffer_db[idx] = user

        return user


    # check and update database for all occurring users
    for issue in issues:
        # check database for issue author
        issue["user"] = get_id_and_update_user(issue["user"])

    # get all users after database updates having been performed
    for issue in issues:
        # get issue author
        issue["user"] = get_user_from_id(issue["user"])

    # dump username, name, and e-mail to file
    lines = []
    for username in username_id_buffer:
        user = get_user_from_id(username_id_buffer[username])
        lines.append((
            username,
            user["name"],
            user["email"]
        ))

    log.info("Dump username list to file...")
    username_dump = os.path.join(resdir, "usernames.list")
    csv_writer.write_to_csv(username_dump, sorted(set(lines), key=lambda line: line[0]))

    return issues

def print_to_disk(issues, results_folder):
    """
    Print issues to file "issues-zulip.list" in the results folder.

    :param issues: the issues to dump
    :param results_folder: the folder where to place "issues-zulip.list" output file
    """

    # construct path to output file
    output_file = os.path.join(results_folder, "issues-zulip.list")
    log.info("Dumping output in file '{}'...".format(output_file))

    # construct lines of output
    lines = []
    for issue in issues:
        # print(issue["user"])
        lines.append((
            issue["discussion_id"],
            issue["discussion_topic"],
            json.dumps(issue["type"]),
            json.dumps([]),
            json.dumps(issue["resolution"]),
            issue["discussion_begin"],
            issue["discussion_end"],
            json.dumps([]),  # components
            issue["individual_events"],
            issue["user"]["name"],
            issue["user"]["email"],
            issue["timestamp"],
            json.dumps([]),
            json.dumps([])
            ))

    # write to output file
    csv_writer.write_to_csv(output_file, sorted(set(lines), key=lambda line: lines.index(line)))