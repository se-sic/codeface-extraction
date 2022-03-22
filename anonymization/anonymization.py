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
# Copyright 2021 by Thomas Bock <bockthom@cs.uni-saarland.de>
# All Rights Reserved.
"""
This file is able to anonymize authors and issue titles after the extraction from the Codeface database was performed.
Author name, e-mail address, message id, and issue title are replaced by an anonymized name (e.g., "developer1",
"developer1@dev.org", "message1@dev.org", etc.) Columns that contain a name, e-mail address, message id, or issue title
are anonymized in author data, commit data, email data, issue data, and bot data.
The resulting anonymized data are written to a separate directory "<resdir>_anonymized" (e.g., if <resdir> is
"threemonth", the anonymized data is written to "threemonth_anonymized").
"""

import argparse
import sys
from os import path, walk, makedirs
from os.path import abspath
from shutil import copy

from codeface.cli import log
from codeface.configuration import Configuration
from codeface.dbmanager import DBManager

from csv_writer import csv_writer


##
# RUN POSTPROCESSING
##

def run_anonymization(conf, resdir):
    """
    Runs the anonymization process for the given parameters, that is, replaces names, e-mail addresses, message ids,
    and issue titles with pseudonymized contents in all .list files in resdir.
    Writes the anonymized .list files to another directory (resdir + "_threemonth").

    :param conf: the Codeface configuration object
    :param resdir: the Codeface results dir, where result files are read from
    """

    authors_list = "authors.list"
    commits_list = "commits.list"
    emails_list = "emails.list"
    issues_github_list = "issues-github.list"
    issues_jira_list = "issues-jira.list"
    bugs_jira_list = "bugs-jira.list"
    bots_list = "bots.list"
    gender_list = "gender.list"
    revisions_list = "revisions.list" # not to be anonymized, only to be copied to the "anonymized" directory

    # When looking at elements originating from json lists, we need to consider quotation marks around the string
    quot_m = "\""

    data_path = path.join(resdir, conf["project"], conf["tagging"])
    anonymize_path = path.join((resdir + "_anonymized"), conf["project"], conf["tagging"])
    if not path.exists(anonymize_path):
        log.info("Create directory %s", anonymize_path)
        makedirs(anonymize_path)

    log.info("%s: Anonymize authors." % conf["project"])

    # create dictionaries to store mappings from authors to anonymized authors and titles to anonymized titles
    author_to_anonymized_author = dict()
    author_to_anonymized_author_gender = dict()
    i = 0
    title_to_anonymized_title = dict()
    k = 0


    """
    Helper function to anonymize author data (i.e., data from the authors.list file).

    :param author_data: the author data to be anonymized (must have been read via "csv_writer.read_from_csv")
    :param i: counter for anonymized developer names (i.e., its current start value which has not been used yet)
    :param author_to_anonymized_author: dictionary in which to lookup and store mappings from (name, e-mail) pairs
                                        to anonymized (name, e-mail) pairs for the developers
    :param name_only: whether also the name (without e-mail) should be used as key for the dictionary
                      "author_to_anonymized_author". This is necessary if there might be lookups using
                      auto-generated and, therefore, different e-mail addresses for the same name.
    :return: the anonymized "author_data",
             the current value of "i" (which has not been used yet),
             and the updated dictionary "author_to_anonymized_author"
    """
    def anonymize_authors(author_data, i, author_to_anonymized_author, name_only = False):

        for author in author_data:
            orig_author = author[1]
            orig_email = author[2]

            # Don't anonymize the deleted user as this one might be needed for filtering (but add it to the dictionary)
            if orig_author == "Deleted user" and orig_email == "ghost@github.com":
                if not (orig_author, orig_email) in author_to_anonymized_author:
                    author_to_anonymized_author[(orig_author, orig_email)] = (orig_author, orig_email)
            else:
                # check whether (name, e-mail) pair isn't already present in the dictionary
                if not (orig_author, orig_email) in author_to_anonymized_author:
                        # check if just the name (without e-mail address) isn't already present in the dictionary
                        if not orig_author in author_to_anonymized_author:
                            # if the author has an empty name, only anonymize their e-mail address
                            if not author[1] == "":
                                author[1] = ("developer" + str(i))
                            author[2] = ("mail" + str(i) + "@dev.org")

                            # add new entry to dictionary (using (name, e-mail) pair as key)
                            author_to_anonymized_author[(orig_author, orig_email)] = (author[1], author[2])
                            # if we allow name-only entries, also add an additional entry to dictionary
                            if name_only:
                                author_to_anonymized_author[orig_author] = (author[1], author[2])

                            # increment counter as we have generated a new anonymized developer id
                            i += 1
                        else:
                            # as just the name (without e-mail address) is present in the dictionary, make a lookup
                            # for the name only and add a new entry to the dictionary using (name, e-mail) pair
                            author_new = author_to_anonymized_author[orig_author]
                            author_to_anonymized_author[(orig_author, orig_email)] = (author_new[0], author_new[1])
                            author[1] = author_new[0]
                            author[2] = author_new[1]
                else:
                    # as the (name, e-mail) pair is present in the dictionary, just make a lookup for the pair
                    author_new = author_to_anonymized_author[(orig_author, orig_email)]
                    author[1] = author_new[0]
                    author[2] = author_new[1]

        return author_data, i, author_to_anonymized_author


    # Check for all files in the result directory of the project whether they need to be anonymized
    for filepath, dirnames, filenames in walk(data_path):

        # (1) Anonymize authors lists
        if authors_list in filenames:
            f = path.join(filepath, authors_list)
            log.info("Anonymize %s ...", f)
            author_data = csv_writer.read_from_csv(f)
            author_data_gender = csv_writer.read_from_csv(f)

            # check if tagging is "feature"
            if conf["tagging"] == "feature":
                # as tagging is "feature", we need to check for the proximity data to keep anonymized ids consistent
                # over both feature and proximity data

                # if corresponding proximity data exists, read authors from proximity data and use them for
                # anonymization to make anonymized proximity data and feature data consistent
                f_proximity = f.replace("feature", "proximity")
                if path.isfile(f_proximity):
                    log.info("Read authors from %s and anonymize them (without dumping to file).", f_proximity)
                    author_data_proximity = csv_writer.read_from_csv(f_proximity)

                    # anonymize authors from proximity data (but just add them to our dictionary, to be used below
                    # for the actual anonymization of the feature data)
                    author_data_proximity, i, author_to_anonymized_author = \
                      anonymize_authors(author_data_proximity, i, author_to_anonymized_author, name_only = True)

            # anonymize authors
            author_data, i, author_to_anonymized_author = \
              anonymize_authors(author_data, i, author_to_anonymized_author)
            i = 0
          
            author_data_gender, i, author_to_anonymized_author_gender = \
              anonymize_authors(author_data_gender, i, author_to_anonymized_author_gender, name_only = True)

            output_path = f.replace(data_path, anonymize_path)
            if not path.exists(path.dirname(output_path)):
                makedirs(path.dirname(output_path))
            log.info("Write anonymized data to %s ...", output_path)
            csv_writer.write_to_csv(output_path, author_data)

        # (2) Anonymize commits lists
        if commits_list in filenames:
            f = path.join(filepath, commits_list)
            log.info("Anonymize %s ...", f)
            commit_data = csv_writer.read_from_csv(f)

            for commit in commit_data:
                # anonymize author
                new_author = author_to_anonymized_author[(commit[2], commit[3])]
                commit[2] = new_author[0]
                commit[3] = new_author[1]
                # anonymize committer
                new_committer = author_to_anonymized_author[(commit[5], commit[6])]
                commit[5] = new_committer[0]
                commit[6] = new_committer[1]

            output_path = f.replace(data_path, anonymize_path)
            if not path.exists(path.dirname(output_path)):
                makedirs(output_path)
            log.info("Write anonymized data to %s ...", output_path)
            csv_writer.write_to_csv(output_path, commit_data)

        # (3) Anonymize emails lists
        if emails_list in filenames:
            f = path.join(filepath, emails_list)
            log.info("Anonymize %s ...", f)
            email_data = csv_writer.read_from_csv(f)

            j = 0

            for email in email_data:
                # anonymize author
                new_author = author_to_anonymized_author[(email[0], email[1])]
                email[0] = new_author[0]
                email[1] = new_author[1]
                # anonymize message id
                email[2] = ("<message" + str(j) + "@message.dev.org>")
                j += 1

            output_path = f.replace(data_path, anonymize_path)
            if not path.exists(path.dirname(output_path)):
                makedirs(path.dirname(output_path))
            log.info("Write anonymized data to %s ...", output_path)
            csv_writer.write_to_csv(output_path, email_data)

        # (4) Anonymize issues lists (github)
        if issues_github_list in filenames:
            f = path.join(filepath, issues_github_list)
            log.info("Anonymize %s ...", f)
            issue_data = csv_writer.read_from_csv(f)

            for issue_event in issue_data:
                # anonymize author
                new_author = author_to_anonymized_author[(issue_event[9], issue_event[10])]
                issue_event[9] = new_author[0]
                issue_event[10] = new_author[1]
                # anonymize person in event info 1/2
                if (issue_event[12], issue_event[13][1:-1]) in author_to_anonymized_author:
                    new_person = author_to_anonymized_author[(issue_event[12], issue_event[13][1:-1])]
                    issue_event[12] = new_person[0]
                    issue_event[13] = quot_m + new_person[1] + quot_m
                # anonymize issue title
                if issue_event[1] in title_to_anonymized_title:
                    issue_event[1] = title_to_anonymized_title[issue_event[1]]
                else:
                    new_title = ("issue-title-" + str(k))
                    title_to_anonymized_title[issue_event[1]] = new_title
                    issue_event[1] = new_title
                    k += 1

            output_path = f.replace(data_path, anonymize_path)
            if not path.exists(path.dirname(output_path)):
                makedirs(path.dirname(output_path))
            log.info("Write anonymized data to %s ...", output_path)
            csv_writer.write_to_csv(output_path, issue_data)

        # (5) Anonymize issues lists (jira)
        if issues_jira_list in filenames:
            f = path.join(filepath, issues_jira_list)
            log.info("Anonymize %s ...", f)
            issue_data = csv_writer.read_from_csv(f)

            for issue_event in issue_data:
                # anonymize author
                new_author = author_to_anonymized_author[(issue_event[9], issue_event[10])]
                issue_event[9] = new_author[0]
                issue_event[10] = new_author[1]
                # anonymize person in event info 1/2
                if (issue_event[12], issue_event[13][1:-1]) in author_to_anonymized_author:
                    new_person = author_to_anonymized_author[(issue_event[12], issue_event[13][1:-1])]
                    issue_event[12] = new_person[0]
                    issue_event[13] = quot_m + new_person[1] + quot_m
                # anonymize issue title
                if issue_event[1] in title_to_anonymized_title:
                    issue_event[1] = title_to_anonymized_title[issue_event[1]]
                else:
                    new_title = ("issue-title-" + str(k))
                    title_to_anonymized_title[issue_event[1]] = new_title
                    issue_event[1] = new_title
                    k += 1

            output_path = f.replace(data_path, anonymize_path)
            if not path.exists(path.dirname(output_path)):
                makedirs(path.dirname(output_path))
            log.info("Write anonymized data to %s ...", output_path)
            csv_writer.write_to_csv(output_path, issue_data)

        # (6) Anonymize bugs lists (jira)
        if bugs_jira_list in filenames:
            f = path.join(filepath, bugs_jira_list)
            log.info("Anonymize %s ...", f)
            bug_data = csv_writer.read_from_csv(f)

            for bug_event in bug_data:
                # anonymize author
                new_author = author_to_anonymized_author[(bug_event[9], bug_event[10])]
                bug_event[9] = new_author[0]
                bug_event[10] = new_author[1]
                # anonymize person in event info 1/2
                if (issue_event[12], issue_event[13][1:-1]) in author_to_anonymized_author:
                    new_person = author_to_anonymized_author[(bug_event[12], bug_event[13][1:-1])]
                    bug_event[12] = new_person[0]
                    bug_event[13] = quot_m + new_person[1] + quot_m
                # anonymize bug title
                if bug_event[1] in title_to_anonymized_title:
                    bug_event[1] = title_to_anonymized_title[bug_event[1]]
                else:
                    new_title = ("issue-title-" + str(k))
                    title_to_anonymized_title[bug_event[1]] = new_title
                    bug_event[1] = new_title
                    k += 1

            output_path = f.replace(data_path, anonymize_path)
            if not path.exists(path.dirname(output_path)):
                makedirs(path.dirname(output_path))
            log.info("Write anonymized data to %s ...", output_path)
            csv_writer.write_to_csv(output_path, bug_data)

        # (7) Anonymize bots list
        if bots_list in filenames:
            f = path.join(filepath, bots_list)
            log.info("Anonymize %s ...", f)
            bot_data = csv_writer.read_from_csv(f)

            for bot in bot_data:
                new_person = author_to_anonymized_author[(bot[0], bot[1])]
                bot[0] = new_person[0]
                bot[1] = new_person[1]

            output_path = f.replace(data_path, anonymize_path)
            if not path.exists(path.dirname(output_path)):
                makedirs(path.dirname(output_path))
            log.info("Write anonymized data to %s ...", output_path)
            csv_writer.write_to_csv(output_path, bot_data)
        
        # (8) Anonymize gender list
        if gender_list in filenames:
            f = path.join(filepath, gender_list)
            log.info("Anonymize %s ...", f)
            gender_data = csv_writer.read_from_csv(f)
            gender_data_new = []

            for author in gender_data:
                if author[0] in author_to_anonymized_author_gender.keys():
                    new_person = author_to_anonymized_author_gender[author[0]]
                    author[0] = new_person[0]
                    gender_data_new.append(author)

            output_path = f.replace(data_path, anonymize_path)
            if not path.exists(path.dirname(output_path)):
                makedirs(path.dirname(output_path))
            log.info("Write anonymized data to %s ...", output_path)
            csv_writer.write_to_csv(output_path, gender_data_new)

        # (9) Copy revisions list
        if revisions_list in filenames:
            f = path.join(filepath, revisions_list)
            log.info("Copy %s ...", f)
            revision_data = csv_writer.read_from_csv(f)

            output_path = f.replace(data_path, anonymize_path)
            if not path.exists(path.dirname(output_path)):
                makedirs(path.dirname(output_path))
            log.info("Copy revision data to %s ...", output_path)
            csv_writer.write_to_csv(output_path, revision_data)


    log.info("Anonymization complete!")


def get_parser():
    """
    Construct parser for the anonymization process.

    :return: the constructed parser
    """
    run_parser = argparse.ArgumentParser(prog='anonymization', description='anonymization')
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

    run_anonymization(__conf, __resdir)


if __name__ == '__main__':
    run()
