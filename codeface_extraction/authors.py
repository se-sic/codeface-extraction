# coding=utf-8
"""
This file provides the needed functions for the authors extraction.
"""

import csv_writer

from os.path import join as pathjoin


def __select_list_of_authors(dbm, project):
    dbm.doExec("""
                    SELECT pers.id AS id, pers.name AS name

                    FROM project p

                    # add authors/developers/persons
                    JOIN person pers
                    ON p.id = pers.projectId

                    # filter for current project
                    WHERE p.name = '%s'

                    ORDER BY pers.id ASC

                    # LIMIT 10
               """ %
               project
               )

    list_of_authors = dbm.doFetchAll()
    return list_of_authors


def get_list_of_authors(dbm, project, project_resdir):
    """
    Selects the list of authors for the given project, using the database-manager parameter.
    Afterwards, the pairs (author_id, author_name) are written to the file 'authors.list' in project_resdir.

    :param dbm: the database manager to use
    :param project: the project name to search
    :param project_resdir: the results directory of the current project
    """

    # get authors for given project
    list_of_authors = __select_list_of_authors(dbm, project)

    # reduce the extracted list (if needed)
    lines = list_of_authors

    # write lines to file
    outfile = pathjoin(project_resdir, "authors.list")
    csv_writer.write_to_csv(outfile, lines)
