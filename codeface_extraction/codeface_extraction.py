# coding=utf-8
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


##
# RUN FOR ALL PROJECTS
##

def run_extraction(conf, resdir):
    """
    Runs the extraction process for the list of given parameters.

    :param conf: the Codeface configuration object
    :param resdir: the Codeface results dir, where output files are written
    """

    log.info("%s: Extracting data" % conf["project"])

    # initialize database manager with given configuration
    dbm = DBManager(conf)

    # get all types of extractions, both project-level and range-level
    __extractions_project, __extractions_range = extractions.get_extractions(dbm, conf, resdir)

    # run project-level extractions
    for extraction in __extractions_project:
        extraction.run()

    # check if list of revisions in database is the same as in the config file
    revs = conf["revisions"]
    list_of_revisions = extractions.RevisionExtraction(dbm, conf, resdir).get_list()
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

    run_extraction(__conf, __resdir)


if __name__ == '__main__':
    run()
