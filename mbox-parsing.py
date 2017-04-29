import mailbox
from joblib import Parallel, delayed
from whoosh.fields import Schema, TEXT, ID
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser
from codeface.configuration import Configuration
from os.path import abspath
import sys
import csv
import os.path
import multiprocessing
import argparse


def parse(mbox_name, results, include_filepath):
    mbox = mailbox.mbox(mbox_name)
    commitList = open(results + "/commits.list", 'r')

    mySchema = Schema(messageID=ID(stored=True), content=TEXT)

    indexPath = results + "/index"
    if not os.path.exists(indexPath):
        print("Creating Index in results folder for text search.")
        os.mkdir(indexPath)
        ix = create_in(indexPath, mySchema)
        ix = open_dir(indexPath)
        writer = ix.writer()
        for message in mbox:
            writer.add_document(messageID=unicode(message['message-id']), content=getbody(message))
        writer.commit()
        print("Index created, parsing will begin now.")

    else:
        print("Index has already been created, parsing will begin right away.")
        ix = open_dir("index")

    commitsSeperated = []
    commits = []
    commitSet = set()
    for line in commitList:
        commits.append(line)

    for commit in commits:
        commitSeperated = str.split(commit, ';')
        commitSet.add((commitSeperated[9], commitSeperated[10]))
        commitsSeperated.append(commitSeperated)
    if include_filepath :
        myFile = open(results + "mboxParsing_filepath.csv", 'w')  # , newline=''
    else :
        myFile = open(results + "mboxParsing.csv", 'w')  # , newline=''
    wr = csv.writer(myFile, delimiter=';')
    wr.writerow(('file', 'function', 'message_id'))
    num_cores = multiprocessing.cpu_count()
    csvData = Parallel(n_jobs=num_cores)(delayed(execute)(commit, mySchema, ix, include_filepath) for commit in commitSet)
    print("Parsing done writing to file commences.")
    for entry in csvData:
        for row in entry:
            wr.writerow(row)
    myFile.close()


def getbody(message):  # getting plain text 'email body'
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


def execute(commit, mySchema, ix, include_filepath):
    result = []
    with ix.searcher() as searcher:
        parser = QueryParser("content", schema=mySchema)
        if include_filepath :
            myquery = parser.parse(commit[0] + " AND " +  commit[1])
        else :
            myquery = parser.parse(commit[1])
        queryResult = searcher.search(myquery, terms=True)
        for r in queryResult:
            touple = (commit[0][1:-1], commit[1][1:-1], r["messageID"])
            result.append(touple)
        print("Artefact " + commit[0][1:-1] + ", " + commit[1][1:-1] + " done!")
        return result


if __name__ == "__main__":
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
        parse(__maildir + "/" + ml["name"] + ".mbox", __resdir + "/" + __project + "_" + __tagging + "/" + __tagging, args.filepath)
