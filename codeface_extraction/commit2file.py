# coding=utf-8
"""
This file provides the needed functions for the artifacts-commit extraction.
"""

import csv_writer

from os.path import join as pathjoin


def __select_files_per_commit(dbm, project, tagging, revision):
    dbm.doExec("""
                    SELECT c.id, cd.file AS file

                    FROM project p

                    # get release range for projects
                    JOIN release_range r ON p.id = r.projectId

                    # start of range
                    JOIN release_timeline l1 ON r.releaseStartId = l1.id
                    # end of range
                    JOIN release_timeline l2 ON r.releaseEndId = l2.id

                    # add commits for the ranges
                    JOIN commit c on r.id = c.releaseRangeId

                    # add meta-data for commits
                    JOIN commit_dependency cd ON c.id = cd.commitId

                    # filter for current release range
                    WHERE p.name = '%s'
                    AND p.analysisMethod = '%s'
                    AND l2.tag = '%s'

                    ORDER BY c.id, cd.file

                    # LIMIT 10
                """ %
               (project, tagging, revision)
               )

    authors_to_artifacts = dbm.doFetchAll()
    return authors_to_artifacts


def get_cochanged_files(dbm, project, tagging, end_rev, range_resdir):
    """
    Selects the list of touched artifacts per commit for the given project, tagging, and release range, using the
    database-manager parameter. Afterwards, the sets are written
     to the file 'cochanged-artifacts.list' in range_resdir.

    :param dbm: the database manager to use
    :param project: the project name to search
    :param tagging: the tagging analysis for the current project
    :param end_rev: the release tag defining the end of a release range
    :param range_resdir: the desired release range of the project
    """

    # get list of changed files per commit
    commit2file = __select_files_per_commit(dbm, project, tagging, end_rev)

    # reduce the extracted list (if needed)
    lines = commit2file

    # write lines to file for current kind of artifact
    outfile = pathjoin(range_resdir, "commit2file.list")
    csv_writer.write_to_csv(outfile, lines)
