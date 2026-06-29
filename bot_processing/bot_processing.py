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
# Copyright 2021-2022 by Thomas Bock <bockthom@cs.uni-saarland.de>
# Copyright 2026 by Leo Sendelbach <s8lesend@stud.uni-saarland.de>
# Copyright 2026 by Thomas Bock <bockthom@cmu.edu>
# Copyright 2025 by Maximilian Löffler <s8maloef@stud.uni-saarland.de>
# All Rights Reserved.
"""
This file is able to extract information on bot/human users from csv files.
"""

import argparse
import os
import sys
from logging import getLogger

from codeface_utils.configuration import Configuration
from codeface_utils.util import setup_logging
from csv_writer import csv_writer
from github_user_utils.github_user_utils import known_copilot_users, generate_botname_variants

# create logger
setup_logging()
log = getLogger(__name__)

def run():
    # get all needed paths and arguments for the method call.
    parser = argparse.ArgumentParser(prog='codeface-extraction-bots-github', description='Codeface extraction')
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

    # get folder that contains known bots file
    # (the known bots file is the file in which known bots have been added manually and project independent)
    __confdir = os.path.join(args.resdir, os.path.dirname(args.config))
    __known_bots_file = os.path.abspath(os.path.join(__confdir, "known_github_bots.list"))
    __known_agents_file = os.path.abspath(os.path.join(__confdir, "known_github_agents.list"))

    # run processing of bot data:
    # 1) load bot data
    bots = load_bot_data(os.path.join(__srcdir, "bots.csv"), header = True)
    # 2) load user data
    users = load_user_data(os.path.join(__resdir, "usernames.list"))
    # 3) update bot data with user data and additionally add known bots if they occur in the project
    bots = add_user_data(bots, users, __known_bots_file, __known_agents_file)
    # 4) dump result to disk
    print_to_disk(bots, __resdir)

    log.info("Bot processing complete!")


def load_bot_data(bot_file, header = True):
    """
    Read list of detected bots and human users from disk.

    :param bot_file: the file which contains information about bot and humans users
    :param header: whether the file to be read contains a header or not (default: True)
    :return: the read bot data
    """

    log.info("Read bot data from file '{}'...".format(bot_file))

    # check if file exists and exit early if not
    if not os.path.exists(bot_file):
        log.error("Bot/Agent file '{}' does not exist (can be empty)! Exiting early...".format(bot_file))
        sys.exit(-1)

    bot_data = csv_writer.read_from_csv(bot_file, delimiter=',')

    if header:
        # remove first line which contains column headers
        bot_data = bot_data[1:]

    return bot_data


def load_user_data(user_data_file):
    """
    Read list of username-to-user mapping from disk.

    :param user_data_file: the file which contains the username-to-user mapping
    :return: the read user data
    """

    log.info("Read user data from file '{}'...".format(user_data_file))

    # check if file exists and exit early if not
    if not os.path.exists(user_data_file):
        log.error("The file '{}' does not exist! Exiting early...".format(user_data_file))
        sys.exit(-1)

    user_data = csv_writer.read_from_csv(user_data_file, delimiter=';')

    return user_data


def check_with_known_bot_or_agent_list(known_bots_file, known_agents_file, bot_data, user_data, bot_data_reduced):
    """
    Check whether there are known bots occurring in the project. If so, add them to the bots list
    or update the bots list accordingly.

    :param known_bots_file: the file path to the list of known bot data
    :param known_agents_file: the file path to the list of known agent data
    :param bot_data: the bot data originating from the bot prediction
    :param user_data: a dictionary from the issue data which maps GitHub usernames to authors
    :param bot_data_reduced: the bot data after mapping GitHub user names to authors
    :return: the bot data as provided in param 'bot_data_reduced' but possibly enriched with
             additional bots (if occurring) or updated bots
    """

    # Read the list of known bots
    known_bots = load_bot_data(known_bots_file, header = False)
    known_agents = load_bot_data(known_agents_file, header = False)

    # Get the GitHub usernames of the bots predicted to be a bot
    predicted_bots = [bot[0] if len(bot) > 0 else "" for bot in bot_data]

    for bot in known_bots:

        # (1) check if a known bot occurs in the GitHub issue data but has not been predicted
        bot_variation_predicted_bots = containing_bot_variation(bot[0], predicted_bots)
        bot_variation_user_data = containing_bot_variation(bot[0], user_data)
        if bot_variation_predicted_bots is None and bot_variation_user_data is not None:

            # add the known bot as a bot to the bots list
            additional_bot = dict()
            additional_bot["user"] = user_data[bot_variation_user_data]
            additional_bot["prediction"] = "Bot"
            bot_data_reduced.append(additional_bot)
            log.info("Add known bot '{}' to bot data.".format(additional_bot["user"]))

        # (2) handle known bots that are already present in the bots list
        elif bot_variation_predicted_bots is not None and bot_variation_user_data is not None:

            # make sure that this bot has also been predicited to be bot
            for predicted_bot in bot_data_reduced:
                if predicted_bot["user"] == user_data[bot_variation_user_data]:
                    predicted_bot["prediction"] = "Bot"
                    log.info("Mark user '{}' as bot in the bot data.".format(user_data[bot_variation_user_data]))
                    break

    # get list of known agents and combine it with the list of known copilot users
    copilot_users_variants = generate_botname_variants(known_copilot_users)
    # get list of known agent names
    known_agents_names = [agent[0] for agent in known_agents]
    for copilot_user in copilot_users_variants:
        if copilot_user not in known_agents_names:
            known_agents.append([copilot_user])

    for agent in known_agents:

        # (1) check if a known agent occurs in the GitHub issue data but has not been predicted
        if agent[0] not in predicted_bots and agent[0] in user_data:

            # add the known agent as a bot to the bots list
            additional_agent = dict()
            additional_agent["user"] = user_data[agent[0]]
            additional_agent["prediction"] = "Agent"
            bot_data_reduced.append(additional_agent)
            log.info("Add known agent '{}' to bot data.".format(additional_agent["user"]))

        # (2) handle known agents that are already present in the bots list
        elif agent[0] in predicted_bots and agent[0] in user_data:

            # make sure that this bot has also been predicited to be an agent
            for predicted_bot in bot_data_reduced:
                if predicted_bot["user"] == user_data[agent[0]]:
                    predicted_bot["prediction"] = "Agent"
                    log.info("Mark user '{}' as agent in the bot data.".format(user_data[agent[0]]))
                    break

    # return the updated bot data
    return bot_data_reduced


def add_user_data(bot_data, user_data, known_bots_file, known_agents_file):
    """
    Add user data to bot data, i.e., replace username by name and e-mail.
    In addition, check in the global bots list whether there are authors in the projects which are
    globally known to be bots. If so, add them to the bots list and update the resulting bots list
    accordingly.

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
        bot_variation = containing_bot_variation(user[0], user_buffer.keys())
        if bot_variation is not None:
            bot_reduced["user"] = user_buffer[bot_variation]
            bot_reduced["prediction"] = user[-1]
            bot_data_reduced.append(bot_reduced)
        else:
            log.warning("User '{}' in bot data does not occur in GitHub user data. Remove user...".format(user[0]))

    # check whether known GitHub bots occur in the GitHub issue data and, if so, update the bot data accordingly
    bot_data_reduced = check_with_known_bot_or_agent_list(known_bots_file, known_agents_file, bot_data, user_buffer, bot_data_reduced)

    return bot_data_reduced


def containing_bot_variation(botname, name_list):
    """
    Helper function to return the variation of a given bot name that occurs in a list of names.

    :param botname: the bot name for which the variation should be returned
    :param name_list: the list of names to be checked for containing the bot name or a variation of it
    :return: the variation of the given bot name that occurs in the given list of names, or None if no such variation exists
    """

    if botname in name_list:
        return botname
    elif botname + "bot" in name_list:
        return botname + "bot"
    elif botname + "[bot]" in name_list:
        return botname + "[bot]"
    elif botname.replace("[", "").replace("]", "") in name_list:
        return botname.replace("[", "").replace("]", "")
    else:
        return None


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
        entry = (user["user"]["name"],
                 user["user"]["email"],
                 user["prediction"]
                )
        if entry not in lines:
            lines.append(entry)

    # write to output file
    csv_writer.write_to_csv(output_file, lines)
