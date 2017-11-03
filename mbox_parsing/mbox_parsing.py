import argparse
import mailbox
import multiprocessing
import os.path
import shutil
import sys
from os.path import abspath

import whoosh.index as index  # import create_in, open_dir, exists_in
from codeface.cli import log
from codeface.configuration import Configuration
from joblib import Parallel, delayed
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser

from codeface_extraction import csv_writer


def __get_index(mbox, results_folder, schema, reindex):
    """Initialize the search index (and create it, if needed

    :param mbox: the file path to the mbox file to create the index for
    :param results_folder: the folder to create the index folder in
    :param schema: the schema for the to be created index
    :param reindex: force reindexing if True
    :return: the opened index object
    """

    # create or load index:
    # 0) construct index path
    index_path = os.path.join(results_folder, "index")
    # 1) if reindexing, remove the index folder
    if os.path.exists(index_path) and reindex:
        shutil.rmtree(index_path)
    # 2) Check if we need to create the index for Whoosh full-text search
    log.devinfo("Checking for index in results folder...")
    if (not os.path.exists(index_path)) or (not index.exists_in(index_path)):
        # 2.1) create index
        log.devinfo("Creating index for text search in results folder.")
        os.mkdir(index_path)  # create path
        index.create_in(index_path, schema)  # initialize as index path
        ix = index.open_dir(index_path)  # open as index path
        writer = ix.writer()
        # add all messages to index
        for message in mbox:
            writer.add_document(messageID=unicode(message['message-id']), content=__mbox_getbody(message))
        writer.commit()
        log.devinfo("Index created, parsing will begin now.")
    else:
        # 2.2) load index
        log.devinfo("Index has already been created, parsing will begin right away.")
        ix = index.open_dir(index_path)

    return ix


# get the search terms from the commits.list file
def __get_artifacts(results_folder):
    """Get the list of artifacts from the list of commits ('commits.list' file)

    :param results_folder: the results folder with the file 'commits.list'
    :return: a set of tuples (file name, artifact)
    """

    commit_set = set()
    with open(os.path.join(results_folder, "commits.list"), 'r') as commit_list:
        for commit in commit_list:
            commit_seperated = str.split(commit, ';')
            commit_set.add((commit_seperated[9], commit_seperated[10]))

    return commit_set


def __mbox_getbody(message):
    """ Gett plain-text e-mail body for better searching.

    :param message: the mbox message to process
    :return: the unicode-encoded message body
    """

    __text_indicator = "text/"

    body = None
    if message.is_multipart():
        for part in message.walk():
            if part.is_multipart():
                for subpart in part.walk():
                    if __text_indicator in subpart.get_content_type():
                        body = subpart.get_payload(decode=True)
            elif __text_indicator in part.get_content_type():
                body = part.get_payload(decode=True)
    elif __text_indicator in message.get_content_type():
        body = message.get_payload(decode=True)

    if body is None:
        log.devinfo(message.get_content_type())
        log.devinfo(
            "An image or some other content has been found that cannot be indexed. Message is given an empty body.")
        body = ' '

    return unicode(body, errors="replace")


def __parse_execute(artifact, schema, index, include_filepath):
    """ Execute the search for the given commit

    :param artifact: the (file name, artifact) tuple to search for
    :param schema: the search schema to use
    :param index: the search index to use
    :param include_filepath: indicator whether to use the 'file name' part of the artifact into account
    :return: a match list of tuples (file name, artifact, message ID)
    """

    log.devinfo("Searching for artifact ({}, {})...".format(artifact[0][1:-1], artifact[1][1:-1]))

    result = []

    with index.searcher() as searcher:
        # initialize query parser
        query_parser = QueryParser("content", schema=schema)

        # construct query
        if include_filepath:
            my_query = query_parser.parse(artifact[0] + " AND " + artifact[1])
        else:
            my_query = query_parser.parse(artifact[1])

        # search!
        query_result = searcher.search(my_query, terms=True)

        # construct result from query answer
        for r in query_result:
            result_tuple = (artifact[0][1:-1], artifact[1][1:-1], r["messageID"])
            result.append(result_tuple)

    return result


def parse(mbox_name, results_folder, include_filepath, reindex):
    """Parse the given mbox file with the commit information from the results folder.

    :param mbox_name: the mbox file to search in
    :param results_folder: the results folder for index and commit information
    :param include_filepath: indicator whether to use the 'file name' part of the artifact into account
    :param reindex: force reindexing if True
    """

    # load mbox file
    mbox = mailbox.mbox(mbox_name)

    # create schema for text search
    schema = Schema(messageID=ID(stored=True), content=TEXT)

    # create/load index (initialize if necessary)
    ix = __get_index(mbox, results_folder, schema, reindex)

    # extract artifacts from results folder
    artifacts = __get_artifacts(results_folder)

    # parallelize execution call for the text search
    log.devinfo("Start parsing...")
    num_cores = multiprocessing.cpu_count()
    csv_data = Parallel(n_jobs=num_cores - 1)(
        delayed(__parse_execute)(commit, schema, ix, include_filepath) for commit in artifacts)
    log.devinfo("Parsing finished.")

    # re-arrange results
    result = [('file', 'artifact', 'messageID')]
    for entry in csv_data:
        for row in entry:
            result.append(row)

    # determine ouput file
    if include_filepath:
        output_file = os.path.join(results_folder, "mboxparsing_filepath.list")
    else:
        output_file = os.path.join(results_folder, "mboxparsing.list")

    # Writes found hits to file.
    log.devinfo("Writing results to file {}.".format(output_file))
    csv_writer.write_to_csv(output_file, result)


def run():
    """Run the mbox-parsing process"""

    # get all needed paths and argument for the method call.
    parser = argparse.ArgumentParser(prog='codeface', description='Codeface extraction')
    parser.add_argument('-c', '--config', help="Codeface configuration file", default='codeface.conf')
    parser.add_argument('-p', '--project', help="Project configuration file", required=True)
    parser.add_argument('-f', '--filepath', help="Include the filepath in the search", action="store_true")
    parser.add_argument('-r', '--reindex', help="Re-construct the index", action="store_true")
    parser.add_argument('resdir', help="Directory to store analysis results in")
    parser.add_argument('maildir', help='Directory in which the mailinglists are located')

    # construct data paths
    args = parser.parse_args(sys.argv[1:])
    __resdir = abspath(args.resdir)
    __maildir = abspath(args.maildir)
    __codeface_conf, __project_conf = map(abspath, (args.config, args.project))

    # initialize configuration
    __conf = Configuration.load(__codeface_conf, __project_conf)
    __resdir_project = os.path.join(__resdir, __conf["project"], __conf["tagging"])

    # search the mailing lists
    for ml in __conf["mailinglists"]:
        mbox_file = os.path.join(__maildir, ml["name"] + ".mbox")
        parse(mbox_file, __resdir_project, args.filepath, args.reindex)


if __name__ == "__main__":
    run()
