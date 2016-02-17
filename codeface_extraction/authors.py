# coding=utf-8
"""
This file provides the needed functions for the authors extraction.
"""

from os.path import join as pathjoin


def __select_list_of_authors(dbm, project):
    dbm.doExec("""
                    SELECT pers.id AS id, pers.name AS name

                    FROM project p

                    # add authors/developers/persons
                    JOIN person pers
                    ON p.id = pers.projectId

                    # filter for current release range and artifact
                    WHERE p.name = '%s'

                    ORDER BY pers.id ASC

                    # LIMIT 10
               """ %
               project
               )

    list_of_authors = dbm.doFetchAll()
    return list_of_authors


def get_list_of_authors(dbm, project, range_resdir):
    """
    Selects the list of authors for the given project, using the database-manager parameter.
    Afterwards, the pairs (author_id, author_name) are written to the file 'authors.list' in range_resdir.

    :param dbm: the database manager to use
    :param project: the project name to search
    :param range_resdir: the desired release range of the project
    """

    # get authors for given project
    list_of_authors = __select_list_of_authors(dbm, project)

    # convert to a proper list for file writing
    lines = ["{}; {}\n".format(dev_id, dev_name) for dev_id, dev_name in list_of_authors]

    # write lines to file
    outfile = pathjoin(range_resdir, "authors.list")
    f = open(outfile, 'w')
    f.writelines(lines)
    f.close()
