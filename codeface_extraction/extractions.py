# coding=utf-8
"""
This file provides the class 'Extraction' and all of its subclasses.
"""

import itertools
import os

from codeface.cli import log


#
# GET EXTRACTIONS
#


def get_extractions(dbm, conf, resdir, csv_writer):
    # all extractions are sublcasses of Extraction:
    # instantiate them all!
    __extractions = []
    for cls in Extraction.__subclasses__():
        __extractions.append(cls(dbm, conf, resdir, csv_writer))

    # group extractions by "project-levelness"
    __extractions_grouped = dict(
        (key, list(e for e in extractions))
        for key, extractions in
        itertools.groupby(__extractions, lambda y: y.is_project_level())
    )
    __extractions_project = __extractions_grouped[True]
    __extractions_range = __extractions_grouped[False]

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

    def _get_out_file(self, start_revision, end_revision, entity_type):
        """
        Return the file path to which the output of this extraction shall be written.
        This methods ensures that the paths are existent.

        :param start_revision: start of an release range (for range-level extractions)
        :param end_revision: end of an release range (for range-level extractions)
        :param entity_type: the artifact to be (probably) encoded in the file name
        :return: the file name
        """

        # get result directory
        dir = os.path.join(
            self.project_res_dir,
            "{0}-{1}".format(start_revision, end_revision)
            if not self.is_project_level()
            else "",
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

    def run(self, start_revision=None, end_revision=None):
        """
        Runs the extraction.

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
                      self._get_out_file(start_revision, end_revision, entity_type)
                      ))

            result = self._run_sql(end_revision, entity_type)
            lines = self._reduce_result(result)
            outfile = self._get_out_file(start_revision, end_revision, entity_type)
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
                    SELECT pers.id AS id, pers.name AS name

                    FROM project p

                    # add authors/developers/persons
                    JOIN person pers
                    ON p.id = pers.projectId

                    # filter for current project
                    WHERE p.name = '{project}'

                    ORDER BY pers.id ASC

                    # LIMIT 10
                """


class CommitExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "commits.list"

        # for subclasses
        self.sql = """
                    SELECT c.id, c.authorDate, a.name, a.email1, c.commitHash,
                           c.ChangedFiles, c.AddedLines, c.DeletedLines, c.DiffSize,
                           cd.file, cd.entityId, cd.entityType, cd.size

                    FROM project p

                    # get commits for project
                    JOIN commit c ON p.id = c.projectId

                    # get commit meta-data
                    JOIN commit_dependency cd ON c.id = cd.commitId

                    # add authors/developers/persons
                    JOIN person a ON c.author = a.id

                    # filter for current project
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'

                    ORDER BY c.authorDate, a.name, c.id, cd.file, cd.entityId

                    # LIMIT 10
                """


class EmailExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "emails.list"

        # for subclasses
        self.sql = """
                    SELECT a.name AS authorName, a.email1, m.messageId, m.creationDate, m.creationDateOffset,
                           m.subject, m.threadId

                    FROM project p

                    # get mails for project
                    JOIN mail m ON p.id = m.projectId

                    # add authors/developers/persons
                    JOIN person a ON m.author = a.id

                    # filter for current project
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'

                    ORDER BY m.threadId, m.creationDate ASC

                    # LIMIT 10
                """


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
        return [rev for (rev,date) in lines]


#
# RANGE-LEVEL EXTRACTIONS
#

class AuthorRangeExtraction(Extraction):
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "authors.list"

        # for subclasses
        self.sql = """
                    SELECT pers.id AS id, pers.name AS name

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


class CommitRangeExtraction(Extraction):
    """This is basically the CommitExtraction, but for one range only."""
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "commits.list"

        # for subclasses
        self.sql = """
                    SELECT c.id, c.authorDate, a.name, a.email1, c.commitHash,
                           c.ChangedFiles, c.AddedLines, c.DeletedLines, c.DiffSize,
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

                    # filter for current project and range
                    WHERE p.name = '{project}'
                    AND p.analysisMethod = '{tagging}'
                    AND l2.tag = '{revision}'

                    ORDER BY c.authorDate, a.name, c.id, cd.file, cd.entityId

                    # LIMIT 10
                """


class EmailRangeExtraction(Extraction):
    """This is basically the EmailExtraction, but for one range only."""
    def __init__(self, dbm, conf, resdir, csv_writer):
        Extraction.__init__(self, dbm, conf, resdir, csv_writer)

        self.file_name = "emails.list"

        # for subclasses
        self.sql = """
                    SELECT a.name AS authorName, a.email1, m.messageId, m.creationDate, m.creationDateOffset,
                           m.subject, m.threadId

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

                    ORDER BY m.threadId, m.creationDate ASC

                    # LIMIT 10
                """
