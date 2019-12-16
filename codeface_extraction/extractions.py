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
# Copyright 2015-2018 by Claus Hunsen <hunsen@fim.uni-passau.de>
# Copyright 2016, 2018-2019 by Thomas Bock <bockthom@fim.uni-passau.de>
# Copyright 2019 by Thomas Bock <bockthom@cs.uni-saarland.de>
# Copyright 2018 by Barbara Eckl <ecklbarb@fim.uni-passau.de>
# Copyright 2018 by Tina Schuh <schuht@fim.uni-passau.de>
# All Rights Reserved.
"""
This file provides the class 'Extraction' and all of its subclasses.
"""

import itertools
import os
import unicodedata
import re
from ftfy import fix_encoding
from email.header import decode_header, make_header

from codeface.cli import log
from codeface.util import gen_range_path


#
# GET EXTRACTIONS
#


def get_extractions(dbm, conf, resdir, csv_writer, extract_commit_messages, extract_impl, extract_on_range_level):
    # all extractions are subclasses of Extraction:
    # instantiate them all!
    __extractions = []

    # check which extractions to skip
    extractions_to_skip = []
    if not extract_commit_messages:
        extractions_to_skip += ["<class 'codeface_extraction.extractions.CommitMessageExtraction'>"]
        extractions_to_skip += ["<class 'codeface_extraction.extractions.CommitMessageRangeExtraction'>"]
    if not extract_impl:
        extractions_to_skip += ["<class 'codeface_extraction.extractions.FunctionImplementationExtraction'>"]
        extractions_to_skip += ["<class 'codeface_extraction.extractions.FunctionImplementationRangeExtraction'>"]

    # collect all extractions (except for the ones to skip) and instantiate objects
    for cls in Extraction.__subclasses__():
        if (str(cls) not in extractions_to_skip):
            __extractions.append(cls(dbm, conf, resdir, csv_writer))

    # group extractions by "project-levelness"
    __extractions_grouped = dict(
        (key, list(e for e in extractions))
        for key, extractions in
        itertools.groupby(__extractions, lambda y: y.is_project_level())
    )

    __extractions_project = __extractions_grouped[True]
    if (extract_on_range_level):
        __extractions_range = __extractions_grouped[False]
    else:
        __extractions_range = []

    return __extractions_project, __extractions_range


#
# EXTRACTION
#

class Extraction(object):
    """
    The class Extraction defines the basic extraction process:
    - run SQL statement,
    - reduce result table if neccessary,
    - get output file depending on extraction level, and
    - write result to output file.
    """

    # map Codeface taggings to artifact types
    _tagging2artifacts = {
        'feature': ['Feature', 'FeatureExpression'],
        'proximity': ['Function']
    }

    def __init__(self, dbm, conf, res_dir, csv_writer):
        """
        Initialize object variables from parameters.

        :param dbm: the database manager
        :param conf: the configuration (parsed from YAML file)
        :param res_dir: the root result directory for the project with :conf
        """

        self.dbm = dbm
        self.conf = conf
        self.project = conf["project"]
        self.revs = conf["revisions"]
        self.tagging = conf["tagging"]
        self.project_res_dir = os.path.join(res_dir, self.project, self.tagging)

        self.file_name = ""

        self.sql = ""

        self.csv_writer = csv_writer

    def is_project_level(self):
        """Check if this extraction is on project level (i.e., {revision} is not on the SQL statement)."""

        return not ("{revision}" in self.sql)

    def is_generic_extraction(self):
        """Check if this extraction is generic (i.e., it can be used for several artifacts and, hence,
        {artifact} is in the file name). """

        return "{artifact}" in self.file_name

    def _run_sql(self, end_revision, entity_type):
        """
        Run the SQL statement.

        :param end_revision: the end of an release range
        :param entity_type: the entity type to be searched
        :return: the result table
        """

        self.dbm.doExec(
            self.sql.format(
                project=self.project,
                tagging=self.tagging,
                revision=end_revision,
                entity_type=entity_type,
                artifact=entity_type
            )
        )
        result = self.dbm.doFetchAll()
        return result

    def _reduce_result(self, result):
        """
        Reduces or rearranges the result.
        :param result: the results as given by the SQL query
        :return: the reduced (or not reduced) result
        """

        return result

    def _get_out_file(self, range_number, start_revision, end_revision, entity_type):
        """
        Return the file path to which the output of this extraction shall be written.
        This methods ensures that the paths are existent.

        :param range_number: the consecutive number of a range (for range-level extractions)
        :param start_revision: start of an release range (for range-level extractions)
        :param end_revision: end of an release range (for range-level extractions)
        :param entity_type: the artifact to be (probably) encoded in the file name
        :return: the file name
        """

        # get result directory
        dir = (gen_range_path(self.project_res_dir, range_number, start_revision, end_revision)
            if not self.is_project_level()
            else os.path.join(self.project_res_dir, "")
        )

        # make sure the result dir exists
        if not os.path.exists(dir):
            os.makedirs(dir)

        # get the file to write inside the result dir
        outfile = os.path.join(
            dir,
            self.file_name.format(
                artifact=entity_type.lower(),
                entity_type=entity_type.lower()
            )
        )

        # and return it
        return outfile

    def _write_export_file(self, lines, outfile):
        """
        Write the given lines to the file given by outfile.
        :param lines: the CSV lines to be written
        :param outfile: the output file
        """

        self.csv_writer.write_to_csv(outfile, lines)

    def run(self, range_number=None, start_revision=None, end_revision=None):
        """
        Runs the extraction.

        :param range_number: the consecutive number of a range (for range-level extractions)
        :param start_revision: start of an release range (for range-level extractions)
        :param end_revision: end of an release range (for range-level extractions)
        """

        artifacts = self._tagging2artifacts[self.tagging]
        if not self.is_generic_extraction():
            artifacts = [artifacts[0]]

        for entity_type in artifacts:
            log.info("%s: %s to %s" %
                     (self.project,
                      self.__class__.__name__,
                      self._get_out_file(range_number, start_revision, end_revision, entity_type)
                      ))

            result = self._run_sql(end_revision, entity_type)
            lines = self._reduce_result(result)
            outfile = self._get_out_file(range_number, start_revision, end_revision, entity_type)
            self._write_export_file(lines, outfile)


#
# PROJECT-LEVEL EXTRACTIONS
#

class AuthorExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "authors.list"

        # for subclasses
        self.sql = """
                    SELECT pers.id AS id, pers.name AS name, pers.email1 AS email

                    FROM project p

                    # add authors/developers/persons
                    JOIN person pers
                    ON p.id = pers.projectId

                    # filter for current project
                    WHERE p.name = '{project}'

                    ORDER BY pers.id ASC

                    # LIMIT 10
                """

    def _reduce_result(self, result):
        # fix name encoding
        return [(id, fix_name_encoding(name), email)
                for (id, name, email) in result]


class CommitExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "commits.list"

        # for subclasses
        self.sql = """
                    SELECT MIN(c.id),
                           c.authorDate, a.name, a.email1,
                           c.commitDate, acom.name, acom.email1,
                           c.commitHash, c.ChangedFiles, c.AddedLines, c.DeletedLines, c.DiffSize,
                           cd.file, cd.entityId, cd.entityType, cd.size

                    FROM project p

                    # get commits for project
                    JOIN commit c ON p.id = c.projectId

                    # get commit meta-data
                    LEFT JOIN commit_dependency cd ON c.id = cd.commitId

                    # add authors/developers/persons
                    JOIN person a ON c.author = a.id

                    # add committers
                    JOIN person acom ON c.committer = acom.id

                    # filter for current project
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'

                    # filter duplicated commits
                    GROUP BY c.commitHash, cd.file, cd.entityId, cd.entityType

                    ORDER BY c.authorDate, a.name, c.id, cd.file, cd.entityId

                    # LIMIT 10
                """

    def _reduce_result(self, result):
        # fix name encoding
        return [(id, authorDate, fix_name_encoding(authorName), authorEmail,
                 commitDate, fix_name_encoding(committerName), committerEmail,
                 commitHash, changedFiles, addedLines, deletedLines, diffSize,
                 file, entityId, entityType, size)
                for (id, authorDate, authorName, authorEmail,
                     commitDate, committerName, committerEmail,
                     commitHash, changedFiles, addedLines, deletedLines, diffSize,
                     file, entityId, entityType, size) in result]


class CommitMessageExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "commitMessages.list"

        # for subclasses
        self.sql = """
                    SELECT MIN(c.id), c.commitHash, c.description

                    FROM project p

                    # get commits for project
                    JOIN commit c ON p.id = c.projectId

                    # filter for current project
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'

                    # filter duplicated commits
                    GROUP BY c.commitHash

                    ORDER BY c.authorDate
                """

    def _reduce_result(self, result):
        # fix character encoding and remove problematic characters from description column
        return [(commitId, commitHash, fix_characters_in_string(description))
                for (commitId, commitHash, description) in result]


# Extraction of function implementations
class FunctionImplementationExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "implementations.list"

        # for subclasses
        self.sql = """
                    SELECT MIN(c.id), c.commitHash,
                           cd.file, cd.entityId, cd.impl

                    FROM project p

                    # get commits for project
                    JOIN commit c ON p.id = c.projectId

                    # get commit meta-data
                    LEFT JOIN commit_dependency cd ON c.id = cd.commitId

                    # filter for current project
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'
                    AND cd.file IS NOT NULL

                    # filter duplicated commits
                    GROUP BY c.commitHash, cd.file, cd.entityId

                    ORDER BY cd.file, cd.entityId, c.id, c.commitHash
                """

    def _reduce_result(self, result):
        # fix character encoding and remove problematic characters from implementation column
        return [(commitId, commitHash, fileId, entityId, fix_characters_in_string(impl))
                for (commitId, commitHash, fileId, entityId, impl) in result]


class EmailExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "emails.list"

        # for subclasses
        self.sql = """
                    SELECT a.name AS authorName, a.email1, m.messageId, m.creationDate, m.creationDateOffset,
                           m.subject, CONCAT(m.mlId, "#", m.threadId) as threadId

                    FROM project p

                    # get mails for project
                    JOIN mail m ON p.id = m.projectId

                    # add authors/developers/persons
                    JOIN person a ON m.author = a.id

                    # filter for current project
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'

                    ORDER BY threadId, m.creationDate ASC

                    # LIMIT 10
                """

    def _reduce_result(self, result):
        # fix name encoding
        return [(fix_name_encoding(name), email, messageId, creationDate, creationDateOffset,
                 subject, threadId)
                for (name, email, messageId, creationDate, creationDateOffset,
                     subject, threadId) in result]


class RevisionExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "revisions.list"

        # for subclasses
        self.sql = """
                    SELECT tag AS revision, t.date AS date

                    FROM project p

                    # add releases
                    JOIN release_timeline t ON p.id = t.projectId

                    # filter for releases
                    WHERE p.name = '{project}'
                    AND t.type = 'release'

                    ORDER BY t.id ASC

                    # LIMIT 10
                """

    def get_list(self):
        result = self._run_sql(None, None)
        lines = self._reduce_result(result)
        return [rev for (rev, date) in lines]


#
# RANGE-LEVEL EXTRACTIONS
#

class AuthorRangeExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "authors.list"

        # for subclasses
        self.sql = """
                    SELECT pers.id AS id, pers.name AS name, pers.email1 AS email

                    FROM project p

                    # add authors/developers/persons
                    JOIN person pers
                    ON p.id = pers.projectId

                    # filter for current project
                    WHERE p.name = '{project}'
                    # {revision} ## hack to get range-level extraction

                    ORDER BY pers.id ASC

                    # LIMIT 10
                """

    def _reduce_result(self, result):
        # fix name encoding
        return [(id, fix_name_encoding(name), email)
                for (id, name, email) in result]


class CommitRangeExtraction(Extraction):
    """This is basically the CommitExtraction, but for one range only."""

    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "commits.list"

        # for subclasses
        self.sql = """
                    SELECT c.id,
                           c.authorDate, a.name, a.email1,
                           c.commitDate, acom.name, acom.email1,
                           c.commitHash, c.ChangedFiles, c.AddedLines, c.DeletedLines, c.DiffSize,
                           cd.file, cd.entityId, cd.entityType, cd.size

                    FROM project p

                    # get release range for projects
                    JOIN release_range r ON p.id = r.projectId

                    # start of range
                    JOIN release_timeline l1 ON r.releaseStartId = l1.id
                    # end of range
                    JOIN release_timeline l2 ON r.releaseEndId = l2.id

                    # add commits for the ranges
                    JOIN commit c ON r.id = c.releaseRangeId

                    # get commit meta-data
                    LEFT JOIN commit_dependency cd ON c.id = cd.commitId

                    # add authors/developers/persons
                    JOIN person a ON c.author = a.id

                    # add committers
                    JOIN person acom ON c.committer = acom.id

                    # filter for current project and range
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'
                    AND l2.tag = '{revision}'

                    # remove commits from previous range
                    AND c.commitHash NOT IN (SELECT c2.commitHash
                                             FROM project p
                                             JOIN release_range r0 ON r0.projectId = p.id
                                             JOIN release_range r1 ON r1.projectId = p.id
                                             JOIN release_timeline l0 ON r0.releaseEndId = l0.id
                                             JOIN release_timeline l2 ON r1.releaseEndId = l2.id
                                             JOIN commit c2 ON r0.id = c2.releaseRangeId
                                             WHERE l2.tag = '{revision}'
                                             AND r0.releaseEndId = r1.releaseStartId
                                             AND p.name = '{project}'
                                             AND p.analysisMethod = '{tagging}' )

                    ORDER BY c.authorDate, a.name, c.id, cd.file, cd.entityId

                    # LIMIT 10
                """

    def _reduce_result(self, result):
        # fix name encoding
        return [(id, authorDate, fix_name_encoding(authorName), authorEmail,
                 commitDate, fix_name_encoding(committerName), committerEmail,
                 commitHash, changedFiles, addedLines, deletedLines, diffSize,
                 file, entityId, entityType, size)
                for (id, authorDate, authorName, authorEmail,
                     commitDate, committerName, committerEmail,
                     commitHash, changedFiles, addedLines, deletedLines, diffSize,
                     file, entityId, entityType, size) in result]


class CommitMessageRangeExtraction(Extraction):
    """This is basically the CommitMessageExtraction, but for one range only."""
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "commitMessages.list"

        # for subclasses
        self.sql = """
                    SELECT c.id, c.commitHash, c.description

                    FROM project p

                    # get release range for projects
                    JOIN release_range r ON p.id = r.projectId

                    # start of range
                    JOIN release_timeline l1 ON r.releaseStartId = l1.id
                    # end of range
                    JOIN release_timeline l2 ON r.releaseEndId = l2.id

                    # add commits for the ranges
                    JOIN commit c ON r.id = c.releaseRangeId

                    # filter for current project
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'
                    AND l2.tag = '{revision}'

                    # remove commits from previous range
                    AND c.commitHash NOT IN (SELECT c2.commitHash
                                             FROM project p
                                             JOIN release_range r0 ON r0.projectId = p.id
                                             JOIN release_range r1 ON r1.projectId = p.id
                                             JOIN release_timeline l0 ON r0.releaseEndId = l0.id
                                             JOIN release_timeline l2 ON r1.releaseEndId = l2.id
                                             JOIN commit c2 ON r0.id = c2.releaseRangeId
                                             WHERE l2.tag = '{revision}'
                                             AND r0.releaseEndId = r1.releaseStartId
                                             AND p.name = '{project}'
                                             AND p.analysisMethod = '{tagging}' )

                    ORDER BY c.authorDate
                """

    def _reduce_result(self, result):
        # fix character encoding and remove problematic characters from description column
        return [(commitId, commitHash, fix_characters_in_string(description))
                for (commitId, commitHash, description) in result]


class EmailRangeExtraction(Extraction):
    """This is basically the EmailExtraction, but for one range only."""

    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "emails.list"

        # for subclasses
        self.sql = """
                    SELECT a.name AS authorName, a.email1, m.messageId, m.creationDate, m.creationDateOffset,
                           m.subject, CONCAT(m.mlId, "#", m.threadId) as threadId

                    FROM project p

                    # get release range for projects
                    JOIN release_range r ON p.id = r.projectId

                    # start of range
                    JOIN release_timeline l1 ON r.releaseStartId = l1.id
                    # end of range
                    JOIN release_timeline l2 ON r.releaseEndId = l2.id

                    # get mails for project
                    JOIN mail m ON p.id = m.projectId

                    # add authors/developers/persons
                    JOIN person a ON m.author = a.id

                    # filter for current release range and artifact
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'
                    AND l2.tag = '{revision}'
                    AND m.creationDate BETWEEN l1.date AND l2.date

                    # remove e-mails from previous range
                    AND m.messageId NOT IN (SELECT m2.messageId
                                            FROM project p
                                            JOIN release_range r0 ON r0.projectId = p.id
                                            JOIN release_range r1 ON r1.projectId = p.id
                                            JOIN release_timeline l0 ON r0.releaseStartId = l0.id
                                            JOIN release_timeline l1 ON r0.releaseEndId = l1.id
                                            JOIN release_timeline l2 ON r1.releaseEndId = l2.id
                                            JOIN mail m2 ON m2.creationDate BETWEEN l0.date AND l1.date
                                            WHERE l2.tag = '{revision}'
                                            AND r0.releaseEndId = r1.releaseStartId
                                            AND p.name = '{project}'
                                            AND p.analysisMethod = '{tagging}' )

                    ORDER BY threadId, m.creationDate ASC

                    # LIMIT 10
                """

    def _reduce_result(self, result):
        # fix name encoding
        return [(fix_name_encoding(name), email, messageId, creationDate, creationDateOffset,
                 subject, threadId)
                for (name, email, messageId, creationDate, creationDateOffset,
                     subject, threadId) in result]


class FunctionImplementationRangeExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "implementations.list"

        # for subclasses
        self.sql = """
                    SELECT c.id, c.commitHash,
                           cd.file, cd.entityId, cd.impl

                    FROM project p

                    # get release range for projects
                    JOIN release_range r ON p.id = r.projectId

                    # start of range
                    JOIN release_timeline l1 ON r.releaseStartId = l1.id
                    # end of range
                    JOIN release_timeline l2 ON r.releaseEndId = l2.id

                    # add commits for the ranges
                    JOIN commit c ON r.id = c.releaseRangeId

                    # get commit meta-data
                    LEFT JOIN commit_dependency cd ON c.id = cd.commitId

                    # filter for current project and range
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'
                    AND l2.tag = '{revision}'
                    AND cd.file IS NOT NULL

                    # remove commits from previous range
                    AND c.commitHash NOT IN (SELECT c2.commitHash
                                             FROM project p
                                             JOIN release_range r0 ON r0.projectId = p.id
                                             JOIN release_range r1 ON r1.projectId = p.id
                                             JOIN release_timeline l0 ON r0.releaseEndId = l0.id
                                             JOIN release_timeline l2 ON r1.releaseEndId = l2.id
                                             JOIN commit c2 ON r0.id = c2.releaseRangeId
                                             WHERE l2.tag = '{revision}'
                                             AND r0.releaseEndId = r1.releaseStartId
                                             AND p.name = '{project}'
                                             AND p.analysisMethod = '{tagging}' )

                    ORDER BY cd.file, cd.entityId, c.id, c.commitHash
                """

    def _reduce_result(self, result):
        # fix character encoding and remove problematic characters from implementation column
        return [(commitId, commitHash, fileId, entityId, fix_characters_in_string(impl))
                for (commitId, commitHash, fileId, entityId, impl) in result]


#
# HELPER FUNCTIONS
#

def fix_characters_in_string(text):
    """
    Removes control characters such as \r\n \x1b \ufffd from string impl and returns a unicode
    string where all control characters have been replaced by a space.
    :param text: expects a unicode string
    :return: unicode string
    """

    # deal with encoding
    new_text = fix_encoding(text)

    # remove unicode characters from "Specials" block
     # see: https://www.compart.com/en/unicode/block/U+FFF0
    new_text = re.sub(r"\\ufff.", " ", new_text.encode("unicode-escape"))

    # remove all kinds of control characters and emojis
    # see: https://www.fileformat.info/info/unicode/category/index.htm
    new_text = u"".join(ch if unicodedata.category(ch)[0] != "C" else " " for ch in new_text.decode("unicode-escape"))

    return new_text


def fix_name_encoding(name):
    """
    Fix encoding of names (originating from e-mail headers).
    :param name: expects a name string extracted from the database
    :return: unicode string of the name (correctly encoded)
    """

    # encode utf-8
    name = name.encode('utf-8')

    # find out character set of the encoded name
    info = decode_header(str(name))

    try:
        # Apply correct encoding and return unicode string
        return unicode(make_header(info))
    except UnicodeDecodeError:
        # Undo utf-8 encoding and return unicode string
        return unicode(name.decode('utf-8'))
    except LookupError:
        # Encoding not found, return string as is
        return name
    return name

