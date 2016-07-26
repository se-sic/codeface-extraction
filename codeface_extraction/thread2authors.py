# coding=utf-8
"""
This file provides the needed functions for the author-email extraction.
"""

from os.path import join as pathjoin


def __select_mailing_authors(dbm, project, tagging, revision):
    dbm.doExec("""
                    SELECT pers.id AS id, pers.name AS name, m.threadId AS thread

                    FROM project p

                    # get release range for projects
                    JOIN release_range r
                    ON p.id = r.projectId

                    # start of range
                    JOIN release_timeline l1
                    ON r.releaseStartId = l1.id
                    # end of range
                    JOIN release_timeline l2
                    ON r.releaseEndId = l2.id

                    # add e-mail data
                    JOIN mail m
                    ON p.id = m.projectId

                    # add authors/developers/persons
                    JOIN person pers
                    ON m.author = pers.id

                    # filter for current release range and artifact
                    WHERE p.name = '%s'
                    AND p.analysisMethod = '%s'
                    AND l2.tag = '%s'
                    AND m.creationDate BETWEEN l1.date AND l2.date

                    ORDER BY m.threadId, m.creationDate ASC

                    #LIMIT 10
                """ %
               (project, tagging, revision)
               )

    authors_to_artifacts = dbm.doFetchAll()
    return authors_to_artifacts


def get_mailing_authors(dbm, project, tagging, end_rev, range_resdir):
    """
    Selects the list of author pairs that exchange e-mails for the given project, tagging, and release range, using the
    database-manager parameter. Afterwards, the sets are written to the file 'thread2authors.list' in range_resdir.

    :param dbm: the database manager to use
    :param project: the project name to search
    :param tagging: the tagging analysis for the current project
    :param end_rev: the release tag defining the end of a release range
    :param range_resdir: the desired release range of the project
    """

    # get list of authors and the threads they contribute to
    author2author = __select_mailing_authors(dbm, project, tagging, end_rev)

    # convert the list to proper csv
    lines = ["{}; {}\n".format(thread, dev_name) for dev_id, dev_name, thread in author2author]

    # write lines to file for current kind of artifact
    # fixme use a separate mail folder?!
    outfile = pathjoin(range_resdir, "thread2authors.list")
    f = open(outfile, 'w')
    f.writelines(lines)
    f.close()
