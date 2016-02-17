# coding=utf-8
"""
This file is able to extract developer--artifact relations from the Codeface database.
"""

from os.path import exists as pathexists, abspath
from os import makedirs
import argparse
import sys

from codeface.cli import log
from codeface.dbmanager import DBManager
from codeface.configuration import Configuration
from authors import *
from author2artifact import *
from thread2authors import *
from commit2artifact import *


##
# RUN FOR ALL PROJECTS
##

def run_extraction(conf, artifact2extraction, resdir):
    """
    Runs the extraction process for the list of given parameters.

    :param conf: the Codeface configuration object
    :param artifact2extraction: a list of pairs (kind of artifact to extract, extraction-process name);
           e.g., [('Feature', 'author2feature')]
    :param resdir: the Codeface results dir, where output files are written
    """

    log.info("%s: Extracting data" % conf["project"])

    # initialize database manager with given configuration
    dbm = DBManager(conf)

    # get setting for current combination
    project = conf["project"]
    project_resdir = conf["repo"]
    revs = conf["revisions"]
    tagging = conf["tagging"]
    project_resdir = pathjoin(resdir, project_resdir, tagging)

    # for all revisions of this project
    for i in range(len(revs) - 1):
        start_rev = revs[i]
        end_rev = revs[i + 1]

        # print (project, tagging, kind, start_rev, end_rev)

        # results directory for current revision
        range_resdir = pathjoin(project_resdir, "{0}-{1}".format(start_rev, end_rev))
        if not pathexists(range_resdir):
            makedirs(range_resdir)

        # get the list of authors in this project
        get_list_of_authors(dbm, project, range_resdir)

        # for all kinds of artifacts that have been analyzed for the current tagging
        for (artifact, extraction) in artifact2extraction:
            log.info("%s: Extracting data: %s" % (conf["project"], extraction))

            # extract the author--artifact list
            get_artifacts_per_author(dbm, project, tagging, extraction, end_rev, artifact, range_resdir)

            # get co-changed artifacts (= artifacts per commit)
            get_cochanged_artifacts(dbm, project, tagging, end_rev, artifact, range_resdir)

        # extract mailing-list analysis (associated with proximity/feature projects!)
        if tagging == 'proximity' or tagging == 'feature':
            log.info("%s: Extracting mailing network for version '%s'" % (conf["project"], end_rev))
            get_mailing_authors(dbm, project, tagging, end_rev, range_resdir)


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

    __artifact2tagging = {
        'feature': [
            ('Feature', 'author2feature'),
            ('FeatureExpression', 'author2featureexpression')
        ],
        'proximity': [
            ('Function', 'author2function')
        ]
        # ('Function', 'author2file')  # FIXME implement author2file (needs new SELECT)
    }

    run_extraction(__conf, __artifact2tagging[__conf["tagging"]], __resdir)


if __name__ == '__main__':
    run()
