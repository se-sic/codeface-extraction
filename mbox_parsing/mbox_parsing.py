import argparse
import csv
import mailbox
import multiprocessing
import os.path
import sys
from os.path import abspath

from codeface.cli import log
from codeface.configuration import Configuration
from joblib import Parallel, delayed
from whoosh.fields import Schema, TEXT, ID
from whoosh.index import create_in, open_dir, exists_in
from whoosh.qparser import QueryParser


def parse(mbox_name, results, include_filepath):
    mbox = mailbox.mbox(mbox_name)

    my_schema = Schema(messageID=ID(stored=True), content=TEXT)
    index_path = results + "/index"
    # The index for Whoosh full text search is being created. If an index already exists this step won't be performed
    if (not os.path.exists(index_path)) or (not exists_in(index_path)):
        log.devinfo("Creating Index in results folder for text search.")
        os.mkdir(index_path)
        ix = create_in(index_path, my_schema)
        ix = open_dir(index_path)
        writer = ix.writer()
        for message in mbox:
            writer.add_document(messageID=unicode(message['message-id']), content=getbody(message))
        writer.commit()
        log.devinfo("Index created, parsing will begin now.")
    else:
        log.devinfo("Index has already been created, parsing will begin right away.")
        ix = open_dir(index_path)

    # Get the search terms from the commits.list file
    commit_list = open(results + "/commits.list", 'r')
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
        my_file = open(results + "mboxParsing_filepath.csv", 'w')
    else:
        my_file = open(results + "mboxParsing.csv", 'w')

    # Paralell execution call for the text search.
    num_cores = multiprocessing.cpu_count()
    csv_data = Parallel(n_jobs=num_cores - 1)(
        delayed(execute)(commit, my_schema, ix, include_filepath) for commit in commit_set)

    # Writes found hits to file.
    log.devinfo("Parsing done writing to file commences.")
    wr = csv.writer(my_file, delimiter=';')
    wr.writerow(('file', 'artifact', 'message_id'))
    for entry in csv_data:
        for row in entry:
            wr.writerow(row)
    my_file.close()
    log.devinfo("Writing done and file closed.")


# Getting plain text 'email body'.
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


# Executes the search for one commit.
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
        log.devinfo("Artifact " + commit[0][1:-1] + ", " + commit[1][1:-1] + " done!")
        return result


def run():
    # Get all needed paths and argument for the method call.
    parser = argparse.ArgumentParser(prog='codeface', description='Codeface extraction')
    parser.add_argument('-c', '--config', help="Codeface configuration file", default='codeface.conf')
    parser.add_argument('-p', '--project', help="Project configuration file", required=True)
    parser.add_argument('-f', '--filepath', help="Include the filepath in the search", action="store_true")
    parser.add_argument('resdir', help="Directory to store analysis results in")
    parser.add_argument('maildir', help='Directory in which the mailinglists are located')

    args = parser.parse_args(sys.argv[1:])
    __resdir = abspath(args.resdir)
    __maildir = abspath(args.maildir)
    __codeface_conf, __project_conf = map(abspath, (args.config, args.project))

    __conf = Configuration.load(__codeface_conf, __project_conf)

    for ml in __conf["mailinglists"]:
        parse(__maildir + "/" + ml["name"] + ".mbox",
              __resdir + "/" + __conf["repo"] + "_" + __conf["tagging"] + "/" + __conf["tagging"],
              args.filepath)


if __name__ == "__main__":
    run()
