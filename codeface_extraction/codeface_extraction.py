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
# Copyright 2016 by Thomas Bock <bockthom@fim.uni-passau.de>
# Copyright 2018 by Barbara Eckl <ecklbarb@fim.uni-passau.de>
# All Rights Reserved.
"""
This file is able to extract developer--artifact relations from the Codeface database.
"""

import argparse
import sys
from os.path import abspath

from codeface.cli import log
from codeface.configuration import Configuration
from codeface.dbmanager import DBManager

import extractions
from csv_writer import csv_writer


##
# RUN FOR ALL PROJECTS
##

def run_extraction(conf, resdir, extract_impl, extract_on_range_level):
    """
    Runs the extraction process for the list of given parameters.

    :param conf: the Codeface configuration object
    :param resdir: the Codeface results dir, where output files are written
    """

    log.info("%s: Extracting data" % conf["project"])

    # initialize database manager with given configuration
    dbm = DBManager(conf)

    # get all types of extractions, both project-level and range-level
    __extractions_project, __extractions_range = extractions.get_extractions(dbm, conf, resdir, csv_writer, extract_impl, extract_on_range_level)

    # run project-level extractions
    for extraction in __extractions_project:
        extraction.run()

    # check if list of revisions in database is the same as in the config file
    revs = conf["revisions"]
    list_of_revisions = extractions.RevisionExtraction(dbm, conf, resdir, csv_writer).get_list()
    if revs:
        if set(revs) != set(list_of_revisions):
            log.error("List of revisions in configuration file do not match the list stored in the DB! Stopping now.")
            sys.exit(1)
        else:
            log.info("List of revisions in configuration file and DB match.")
    else:
        log.info("No list of revisions found in configuration file, using the list from the DB instead!")
        revs = list_of_revisions  # set list of revisions as stored in the database

    # for all revisions of this project
    for i in range(len(revs) - 1):
        start_rev = revs[i]
        end_rev = revs[i + 1]

        log.info("%s: Extracting data for version '%s'" % (conf["project"], end_rev))

        for extraction in __extractions_range:
            extraction.run(start_rev, end_rev)

    log.info("Extraction complete!")


def get_parser():
    """
    Construct parser for the extraction process.

    :return: the constructed parser
    """
    run_parser = argparse.ArgumentParser(prog='codeface', description='Codeface extraction')
    run_parser.add_argument('-c', '--config', help="Codeface configuration file",
                            default='codeface.conf')
    run_parser.add_argument('-p', '--project', help="Project configuration file",
                            required=True)
    run_parser.add_argument('-i', '--implementation', help="Enable extraction of the source code of functions",
                            action='store_true')
    run_parser.add_argument('-r', '--range', help="Enable extraction of the data on range level",
                            action='store_true')
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
    __extract_impl = args.implementation
    __extract_on_range_level = args.range

    # load configuration
    __conf = Configuration.load(__codeface_conf, __project_conf)

    run_extraction(__conf, __resdir, __extract_impl, __extract_on_range_level)


if __name__ == '__main__':
    run()
