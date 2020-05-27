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
# Copyright 2015-2017 by Claus Hunsen <hunsen@fim.uni-passau.de>
# Copyright 2020 by Thomas Bock <bockthom@cs.uni-saarland.de>
# All Rights Reserved.
"""
This file is able to disambiguate authors after the extraction from the Codeface database was performed. A manually
created disambiguation file is used to disambiguate the authors in all the extracted files of a project.

The manually created disambiguation file 'disambiguation-after-db.list' has to have the following format:
    - each line combines two person identities which should be mapped to each other
    - each line consists of six columns (each three for describing id, name, e-mail address)
    - the entries of each line are taken from the global 'authors.list' file
    - Example:
        1234;Claus Hunsen;claus.hunsen@example.org;5678;claushunsen;hunsen.claus@example.net;
    - the first three columns of a line describe the person identity to keep (e.g., 1234)
    - the last three columns of a line describe the person identity to replace (e.g., 5678)
Result: Every occurrence of the second person identity will be replaced by the first person identity, in every .list
file of the project (authors, commits, emails, issues, etc.)

If more than two person identities should be mapped to each other, several lines are necessary in the disambiguation
file. E.g., if persons A, B, C should be mapped to A, there has to be a line which replaces B by A (A,B) and a line
which replaces C by A (A,C).
"""

import argparse
import sys
from os import path, walk
from os.path import abspath

from codeface.cli import log
from codeface.configuration import Configuration
from codeface.dbmanager import DBManager

from csv_writer import csv_writer


##
# RUN POSTPROCESSING
##


def run_postprocessing(conf, resdir):
    """
    Runs the postprocessing for the given parameters, that is, read the disambiguation file of the project
    and replace all author names and e-mail addresses in all other .list files according to the disambiguation file

    :param conf: the Codeface configuration object
    :param resdir: the Codeface results dir, where output files are written
    """

    log.info("%s: Postprocess authors after manual disambiguation" % conf["project"])

    authors_list = "authors.list"
    commits_list = "commits.list"
    emails_list = "emails.list"
    issues_github_list = "issues-github.lisst"
    issues_jira_list = "issues-jira.list"
    bugs_jira_list = "bugs-jira.list"

    disambiguation_list = path.join(resdir, conf["project"], conf["tagging"], "disambiguation-after-db.list")

    # Check if a disambiguation list exists - if not, just stop
    if path.exists(disambiguation_list):
        disambiguation_data = csv_writer.read_from_csv(disambiguation_list)
    else:
        log.info("Disambiguation file does not exist: %s", disambiguation_list)
        log.info("No postprocessing performed!")
        return

    # Check for all files in the result directory of the project whether they need to be adjusted
    for filepath, dirnames, filenames in walk(path.join(resdir, conf["project"], conf["tagging"])):
        
        # (1) Adjust authors lists
        if authors_list in filenames:
            f = path.join(filepath, authors_list)
            log.info("Postprocess %s ...", f)
            author_data = csv_writer.read_from_csv(f)

            author_data_to_remove = []
            author_data_new = []

            # get persons which should be removed
            for person in disambiguation_data:
                author_data_to_remove.append([person[3], person[4], person[5]])

            for author in author_data:
                # keep author entry only if it should not be removed
                if not author in author_data_to_remove:
                    author_data_new.append(author)
            csv_writer.write_to_csv(f, author_data_new)

        # (2) Adjust commits lists
        if commits_list in filenames:
            f = path.join(filepath, commits_list)
            log.info("Postprocess %s ...", f)
            commit_data = csv_writer.read_from_csv(f)

            for person in disambiguation_data:
                for commit in commit_data:
                    # replace author if necessary
                    if person[4] == commit[2] and person[5] == commit[3]:
                        commit[2] = person[1]
                        commit[3] = person[2]
                    # replace committer if necessary
                    if person[4] == commit[5] and person[5] == commit[6]:
                        commit[5] = person[1]
                        commit[6] = person[2]

            csv_writer.write_to_csv(f, commit_data)

        # (3) Adjust emails lists
        if emails_list in filenames:
            f = path.join(filepath, emails_list)
            log.info("Postprocess %s ...", f)
            email_data = csv_writer.read_from_csv(f)

            for person in disambiguation_data:
                for email in email_data:
                    # replace author if necessary
                    if person[4] == email[0] and person[5] == email[1]:
                        email[0] = person[1]
                        email[1] = person[2]

            csv_writer.write_to_csv(f, email_data)

        # (4) Adjust issues lists (github)
        if issues_github_list in filenames:
            f = path.join(filepath, issues_github_list)
            log.info("Postprocess %s ...", f)
            issue_data = csv_writer.read_from_csv(f)

            for person in disambiguation_data:
                for issue_event in issue_data:
                    # replace author if necessary
                    if person[4] == issue_event[9] and person[5] == issue_event[10]:
                        issue_event[9] = person[1]
                        issue_event[10] = person[2]
                    # replace person in event info 1/2 if necessary
                    if person[4] == issue_event[12] and person[5] == issue_event[13]:
                        issue_event[12] = person[1]
                        issue_event[13] = person[2]

            csv_writer.write_to_csv(f, issue_data)

        # (5) Adjust issues lists (jira)
        if issues_jira_list in filenames:
            f = path.join(filepath, issues_jira_list)
            log.info("Postprocess %s ...", f)
            issue_data = csv_writer.read_from_csv(f)

            for person in disambiguation_data:
                for issue_event in issue_data:
                    # replace author if necessary
                    if person[4] == issue_event[9] and person[5] == issue_event[10]:
                        issue_event[9] = person[1]
                        issue_event[10] = person[2]
                    # replace person in event info 1/2 if necessary
                    if person[4] == issue_event[12] and person[5] == issue_event[13]:
                        issue_event[12] = person[1]
                        issue_event[13] = person[2]

            csv_writer.write_to_csv(f, issue_data)

        # (6) Adjust bugs lists (jira)
        if bugs_jira_list in filenames:
            f = path.join(filepath, bugs_jira_list)
            log.info("Postprocess %s ...", f)
            bug_data = csv_writer.read_from_csv(f)

            for person in disambiguation_data:
                for bug_event in bug_data:
                    # replace author if necessary
                    if person[4] == bug_event[9] and person[5] == bug_event[10]:
                        bug_event[9] = person[1]
                        bug_event[10] = person[2]
                    # replace person in event info 1/2 if necessary
                    if person[4] == bug_event[12] and person[5] == bug_event[13]:
                        bug_event[12] = person[1]
                        bug_event[13] = person[2]

            csv.writer.write_to_csv(f, bug_data)

    log.info("Postprocessing complete!")


def get_parser():
    """
    Construct parser for the postprocessing process.

    :return: the constructed parser
    """
    run_parser = argparse.ArgumentParser(prog='postprocessing', description='postprocessing')
    run_parser.add_argument('-c', '--config', help="Codeface configuration file",
                            default='codeface.conf')
    run_parser.add_argument('-p', '--project', help="Project configuration file",
                            required=True)
    run_parser.add_argument('resdir',
                            help="Directory to store analysis results in")

    return run_parser


def run():
    # get Codeface parser
    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])  # Note: The first argument of argv is the name of the command

    # process arguments
    # - First make all the args absolute
    __resdir = abspath(args.resdir)
    __codeface_conf, __project_conf = map(abspath, (args.config, args.project))

    # load configuration
    __conf = Configuration.load(__codeface_conf, __project_conf)

    run_postprocessing(__conf, __resdir)


if __name__ == '__main__':
    run()
