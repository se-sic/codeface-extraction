import argparse
import os
import sys
from os.path import abspath

from codeface.configuration import Configuration
from codeface.dbmanager import DBManager


def run():

    parser = argparse.ArgumentParser(prog='codeface', description='Codeface extraction')
    parser.add_argument('-c', '--config', help="Codeface configuration file", default='codeface.conf')
    parser.add_argument('-p', '--project', help="Project configuration file", required=True)
    parser.add_argument('resdir', help="Directory to store analysis results in")
    args = parser.parse_args(sys.argv[1:])

    __resdir = abspath(args.resdir)
    __codeface_conf, __project_conf = map(abspath, (args.config, args.project))
    __conf = Configuration.load(__codeface_conf, __project_conf)
    _dbm = DBManager(__conf)
    project_id = _dbm.getProjectID(__conf["project"], __conf["tagging"])
    id_service_url = __conf['idServiceHostname'] + ':' + __conf['idServicePort']

    os.system("java -jar IssuesTracking.jar " + project_id + " " + id_service_url + " " + __resdir)
    return None
