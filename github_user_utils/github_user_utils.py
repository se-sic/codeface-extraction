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
# Copyright 2026 by Leo Sendelbach <s8lesend@stud.uni-saarland.de>
# All Rights Reserved.
"""
This file serves as a collection of global variables and utility functions, which are used throughout the 
issue data extraction and post-processing, in particular for the processing of GitHub and Copilot user data.
"""

##
# GLOBAL VARIABLES
##

# global variables containing all known copilot users and the name and mail address copilot users will be assigned
known_copilot_users = {"Copilot", "copilot-pull-request-reviewer[bot]", "copilot-swe-agent[bot]"}
copilot_unified_name = "Copilot"
copilot_unified_email = "copilot@example.com"

## global variables for the GitHub author
github_user = "GitHub"
github_email = "noreply@github.com"
commit_added_event = "commit_added"
mentioned_event = "mentioned"
subscribed_event = "subscribed"
assigned_event = "assigned"
unassigned_event = "unassigned"
review_requested_event = "review_requested"
review_request_removed_event = "review_request_removed"

# When looking at elements originating from json lists, we need to consider quotation marks around the string
quot_m = "\""

##
# UTILITY FUNCTIONS
##

def is_github_noreply_author(name, email):
    """
    Helper function to check whether a (name, e-mail) pair belongs to the author "GitHub <noreply@github.com>".
    There are two options in Codeface how this can happen:
    (1) Username is "GitHub" and e-mail address is "noreply@github.com"
    (2) Username is "GitHub" and e-mail address has been replaced by Codeface, resulting in "GitHub.noreply@github.com"

    :param name: the name of the author to be checked
    :param email: the email address of the author to be checked
    :return: whether the given (name, email) pair belongs to the "GitHub <noreply@github.com>" author
    """

    return (name == github_user and (email == github_email or email == (github_user + "." + github_email)))

def generate_botname_variants(botnames):
    """
    Helper function to generate variants of bot names, which are used in the list of
    known bots and agents as well as during author postprocessing.

    :param botnames: the list of bot names for which variants should be generated
    :return: a set of bot name variants
    """

    botname_variants = set()
    for botname in botnames:
        botname_variants.add(botname)
        botname = botname.replace("[", "").replace("]", "")
        botname_variants.add(botname)

    return botname_variants
