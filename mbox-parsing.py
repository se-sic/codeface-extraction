import mailbox
from joblib import Parallel, delayed
from whoosh.fields import Schema, TEXT, ID
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser
from codeface.configuration import Configuration
from os.path import abspath
from codeface.cli import log
import sys
import csv
import os.path
import multiprocessing
import argparse


def parse(mbox_name, results, include_filepath):
    mbox = mailbox.mbox(mbox_name)
    commit_list = open(results + "/commits.list", 'r')

    # The index for Whoosh full text search is being created. If an index already exists this step won't be performed
    # TODO can lead to problems if the index needs to be updated or creation was aborted.
    my_schema = Schema(messageID=ID(stored=True), content=TEXT)
    index_path = results + "/index"
    if not os.path.exists(index_path):
        log.info("Creating Index in results folder for text search.")
        os.mkdir(index_path)
        ix = create_in(index_path, my_schema)
        ix = open_dir(index_path)
        writer = ix.writer()
        for message in mbox:
            writer.add_document(messageID=unicode(message['message-id']), content=getbody(message))
        writer.commit()
        log.info("Index created, parsing will begin now.")
    else:
        log.info("Index has already been created, parsing will begin right away.")
        ix = open_dir("index")

    commits_seperated = []
    commits = []
    commit_set = set()
    for line in commit_list:
        commits.append(line)
    for commit in commits:
        commit_seperated = str.split(commit, ';')
        commit_set.add((commit_seperated[9], commit_seperated[10]))
        commits_seperated.append(commit_seperated)
    if include_filepath:
        my_file = open(results + "mboxParsing_filepath.csv", 'w')  # , newline=''
    else:
        my_file = open(results + "mboxParsing.csv", 'w')  # , newline=''
    wr = csv.writer(my_file, delimiter=';')
    wr.writerow(('file', 'function', 'message_id'))
    # Paralell execution call for the main text search.
    num_cores = multiprocessing.cpu_count()
    csv_data = Parallel(n_jobs=num_cores - 1)(
        delayed(execute)(commit, my_schema, ix, include_filepath) for commit in commit_set)
    log.info("Parsing done writing to file commences.")
    for entry in csv_data:
        for row in entry:
            wr.writerow(row)
    my_file.close()
    log.info("Writing done and file closed.")


# Getting plain text 'email body'
def getbody(message):
    body = None
    if message.is_multipart():
        for part in message.walk():
            if part.is_multipart():
                for subpart in part.walk():
                    if subpart.get_content_type() == 'text/plain':
                        body = subpart.get_payload(decode=True)
            elif part.get_content_type() == 'text/plain':
                body = part.get_payload(decode=True)
    elif message.get_content_type() == 'text/plain':
        body = message.get_payload(decode=True)
    return unicode(body, errors="replace")


# Executes the search for one artifact.
def execute(commit, my_schema, ix, include_filepath):
    result = []
    with ix.searcher() as searcher:
        query_parser = QueryParser("content", schema=my_schema)
        if include_filepath:
            my_query = query_parser.parse(commit[0] + " AND " + commit[1])
        else:
            my_query = query_parser.parse(commit[1])
        query_result = searcher.search(my_query, terms=True)
        for r in query_result:
            result_tuple = (commit[0][1:-1], commit[1][1:-1], r["messageID"])
            result.append(result_tuple)
        log.info("Artifact " + commit[0][1:-1] + ", " + commit[1][1:-1] + " done!")
        return result


if __name__ == "__main__":
    # Get all needed paths and argument for the method call.
    parser = argparse.ArgumentParser(prog='codeface', description='Codeface extraction')
    parser.add_argument('-c', '--config', help="Codeface configuration file", default='codeface.conf')
    parser.add_argument('-p', '--project', help="Project configuration file", required=True)
    parser.add_argument('-f', '--filepath', help="Include the filepath in the search", action="store_true")
    parser.add_argument('resdir', help="Directory to store analysis results in")
    parser.add_argument('maildir', help='Direcotry in which the mailinglists are located')
    parser.add_argument('projectname', help='Name of the project')
    parser.add_argument('tagging', help='Current tagging of the analysis')

    args = parser.parse_args(sys.argv[1:])
    __resdir = abspath(args.resdir)
    __maildir = abspath(args.maildir)
    __project = args.projectname
    __tagging = args.tagging
    __codeface_conf, __project_conf = map(abspath, (args.config, args.project))

    __conf = Configuration.load(__codeface_conf, __project_conf)

    for ml in __conf["mailinglists"]:
        parse(__maildir + "/" + ml["name"] + ".mbox", __resdir + "/" + __project + "_" + __tagging + "/" + __tagging,
              args.filepath)
