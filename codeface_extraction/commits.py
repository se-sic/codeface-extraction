# coding=utf-8
"""
This file provides the needed functions for the commit extraction.
"""

import csv_writer

from os.path import join as pathjoin


def __select_commits(dbm, project, tagging):
    dbm.doExec("""
                    SELECT c.id, c.authorDate, a.name, a.email1, c.commitHash,
                           c.ChangedFiles, c.AddedLines, c.DeletedLines, c.DiffSize,
                           cd.file, cd.entityId, cd.entityType, cd.size

                    FROM project p

                    # get commits for project
                    JOIN commit c
                    ON p.id = c.projectId

                    # get commit meta-data
                    JOIN commit_dependency cd
                    ON c.id = cd.commitId

                    # add authors/developers/persons
                    JOIN person a
                    ON c.author = a.id

                    # filter for current project
                    WHERE p.name = '%s'
                    AND p.analysisMethod = '%s'

                    ORDER BY c.authorDate, a.name, c.id, cd.file, cd.entityId

                    # LIMIT 10
                """ %
               (project, tagging)
               )

    commits = dbm.doFetchAll()
    return commits


def get_list_of_commits(dbm, project, tagging, project_resdir):
    """
    Selects the list of commits for the given project, and tagging, using the
    database-manager parameter. Afterwards, the pairs
    (author_name, file_name) are written to the file '[kind].list' in range_resdir.

    :param dbm: the database manager to use
    :param project: the project name to search
    :param tagging: the tagging analysis for the current project
    """

    # get commit information
    commits = __select_commits(dbm, project, tagging)

    # reduce the extracted list (if needed)
    lines = commits

    # write lines to file
    outfile = pathjoin(project_resdir, "commits.list")
    csv_writer.write_to_csv(outfile, lines)
