# coding=utf-8
"""
This file provides the needed functions for the email extraction.
"""

from os.path import join as pathjoin


def __select_emails(dbm, project, tagging):
    dbm.doExec("""
                    SELECT a.name AS authorName, a.email1, m.creationDate, m.subject, m.threadId

                    FROM project p

                    JOIN mail m

                    ON p.id = m.projectId 

                    JOIN person a 

                    ON m.author=a.id

                    WHERE p.name = '%s'

                    AND p.analysisMethod = '%s'

                    ORDER BY authorName, creationDate

                    # LIMIT 10
                """ %
               (project, tagging)
               )

    emails = dbm.doFetchAll()
    return emails


def get_list_of_emails(dbm, project, tagging, project_resdir):
    """
    Selects the list of emails for the given project, and tagging, using the
    database-manager parameter. Afterwards, the pairs
    (author_name, file_name) are written to the file '[kind].list' in range_resdir.

    :param dbm: the database manager to use
    :param project: the project name to search
    :param tagging: the tagging analysis for the current project
    """

    # get email information
    emails = __select_emails(dbm, project, tagging)

    # convert emails to tuples
    lines = ["{}; {}; {}\n".format(authorName, creationDate, threadId ) for authorName, creationDate, threadId in emails]

    # write lines to file
    outfile = pathjoin(project_resdir, "emails.list")
    f = open(outfile, 'w')
    f.writelines(lines)
    f.close()
