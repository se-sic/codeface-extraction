# coding=utf-8
"""
This file is able to extract developer--artifact relations from the Codeface database.
"""

import argparse
import sys
from os import makedirs
from os.path import exists as pathexists, abspath

from codeface.cli import log
from codeface.configuration import Configuration
from codeface.dbmanager import DBManager

from author2artifact import *
from author2file import *
from authors import *
from commit2artifact import *
from commit2file import *
from revisions import *
from thread2authors import *
from commits import *
from emails import *


##
# RUN FOR ALL PROJECTS
##

def run_extraction(conf, artifacts, resdir):
    """
    Runs the extraction process for the list of given parameters.

    :param conf: the Codeface configuration object
    :param artifacts: a list of artifacts to extract, e.g., ['Feature', 'FeatureExpression']
    :param resdir: the Codeface results dir, where output files are written
    """

    log.info("%s: Extracting data" % conf["project"])

    # initialize database manager with given configuration
    dbm = DBManager(conf)

    # get setting for current combination
    project = conf["project"]
    project_resdir = conf["project"]
    revs = conf["revisions"]
    tagging = conf["tagging"]
    project_resdir = pathjoin(resdir, project_resdir, tagging)

    # create results directory
    if not pathexists(project_resdir):
        makedirs(project_resdir)

    # extract list of revisions as stored in the database
    list_of_revisions = get_list_of_revisions(dbm, project, project_resdir)
    # get the list of authors in this project
    get_list_of_authors(dbm, project, project_resdir)
    # get the list of commits in this project
    get_list_of_commits(dbm, project, tagging, project_resdir)
    # get the list of emails in this project
    get_list_of_emails(dbm, project, tagging, project_resdir)

    # check if list of revisions in database is the same as in the config file
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

        # print (project, tagging, kind, start_rev, end_rev)

        # results directory for current revision
        range_resdir = pathjoin(project_resdir, "{0}-{1}".format(start_rev, end_rev))
        if not pathexists(range_resdir):
            makedirs(range_resdir)

        # for all kinds of artifacts that have been analyzed for the current tagging
        for artifact in artifacts:
            log.info("%s: Extracting data: %s" % (conf["project"], artifact))

            # extract the author--artifact list
            get_artifacts_per_author(dbm, project, tagging, end_rev, artifact, range_resdir)

            # get co-changed artifacts (= artifacts per commit)
            get_cochanged_artifacts(dbm, project, tagging, end_rev, artifact, range_resdir)

        if tagging == 'proximity' or tagging == 'feature':
            # extract mailing-list analysis (associated with proximity/feature projects!)
            log.info("%s: Extracting mailing network for version '%s'" % (conf["project"], end_rev))
            get_mailing_authors(dbm, project, tagging, end_rev, range_resdir)

            # extract author2file mapping (embedded into proximity/feature projects!)
            log.info("%s: Extracting file network for version '%s'" % (conf["project"], end_rev))
            get_files_per_author(dbm, project, tagging, end_rev, range_resdir)
            get_cochanged_files(dbm, project, tagging, end_rev, range_resdir)


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

    __tagging2artifacts = {
        'feature': ['Feature', 'FeatureExpression'],
        'proximity': ['Function']
    }

    run_extraction(__conf, __tagging2artifacts[__conf["tagging"]], __resdir)


if __name__ == '__main__':
    run()