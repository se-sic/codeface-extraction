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
# Copyright 2021 by Thomas Bock <bockthom@cs.uni-saarland.de>
# All Rights Reserved.
"""
This file is able to extract information on bot/human users from csv files.
"""

import argparse
import httplib
import os
import sys
import urllib

import operator
from codeface.cli import log
from codeface.configuration import Configuration

from csv_writer import csv_writer

def run():
    # get all needed paths and arguments for the method call.
    parser = argparse.ArgumentParser(prog='codeface-extraction-bots-github', description='Codeface extraction')
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

    # run processing of bot data:
    # 1) load bot data
    bots = load_bot_data(os.path.join(__srcdir, "bots.csv"))
    # 2) load user data
    users = load_user_data(os.path.join(__srcdir, "usernames.list"))
    # 3) update bot data with user data
    bots = add_user_data(bots, users)
    # 4) dump result to disk
    print_to_disk(bots, __resdir)

    log.info("Bot processing complete!")


def load_bot_data(bot_file):
    """Read list of detected bots and human users from disk.

    :param bot_file: the file which contains information about bot and humans users
    :return: the read bot data
    """

    log.devinfo("Read bot data from file '{}'...".format(bot_file))

    # check if file exists and exit early if not
    if not os.path.exists(bot_file):
        log.error("Bot file '{}' does not exist! Exiting early...".format(bot_file))
        sys.exit(-1)

    bot_data = csv_writer.read_from_csv(bot_file, delimiter=',')

    # remove first line which contains column headers
    bot_data = bot_data[1:]

    return bot_data


def load_user_data(user_data_file):
    """
    Read list of username-to-user mapping from disk.

    :param user_data_file: the file which contains the username-to-user mapping
    :return: the read user data
    """

    log.devinfo("Read user data from file '{}'...".format(user_data_file))

    # check if file exists and exit early if not
    if not os.path.exists(user_data_file):
        log.error("The file '{}' does not exist! Exiting early...".format(user_data_file))
        sys.exit(-1)

    user_data = csv_writer.read_from_csv(user_data_file, delimiter=';')

    return user_data


def add_user_data(bot_data, user_data):
    """
    Add user data to bot data, i.e., replace username by name and e-mail.

    :param bot_data: the list of detected bot/human users
    :param user_data: the list of usernames to retrieve user data from
    :return: the updated bot data
    """

    log.info("Syncing usernames with resolved user data...")

    # create buffer for users (key: username)
    user_buffer = dict()

    # update user buffer for all users
    for user in user_data:
        info = dict()
        info["name"] = user[1]
        info["email"] = user[2]
        user_buffer[user[0]] = info

    # dictionary for all rows of the bot data with reduced number of columns
    bot_data_reduced = list()

    # update all bots
    for user in bot_data:
        bot_reduced = dict()

        # bot data might contain empty lines, which need to be ignored
        if len(user) == 0:
            continue

        # get user information if available
        if user[0] in user_buffer.keys():
            bot_reduced["user"] = user_buffer[user[0]]
            bot_reduced["prediction"] = user[-1]
            bot_data_reduced.append(bot_reduced)
        else:
            log.warn("User '{}' in bot data does not occur in GitHub user data. Remove user...".format(user[0]))

    return bot_data_reduced


def print_to_disk(bot_data, results_folder):
    """
    Print bot data to file "bots.list" in result folder.

    :param bot_data: the bot data to dump
    :param results_folder: the folder where to place "bots.list" output file
    """

    # construct path to output file
    output_file = os.path.join(results_folder, "bots.list")
    log.info("Dumping output in file '{}'...".format(output_file))

    # construct lines of output
    lines = []
    for user in bot_data:
        lines.append((
             user["user"]["name"],
             user["user"]["email"],
             user["prediction"]
        ))

    # write to output file
    csv_writer.write_to_csv(output_file, lines)
