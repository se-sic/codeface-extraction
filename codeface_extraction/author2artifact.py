# coding=utf-8
"""
This file provides the needed functions for the authors extraction.
"""

from os.path import join as pathjoin


def __select_artifacts_per_author(dbm, project, tagging, revision, entitytype="FEATURE"):
    dbm.doExec("""
                    SELECT pers.id AS id, pers.name AS name, cd.entityId AS artifact

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

                    # add authors/developers/persons
                    JOIN person pers ON c.author = pers.id

                    # filter for current release range and artifact
                    WHERE p.name = '%s'
                    AND p.analysisMethod = '%s'
                    AND l2.tag = '%s'
                    AND cd.entityType = '%s'

                    GROUP BY name, artifact ASC
                    ORDER BY id, artifact ASC

                    # LIMIT 10
                """ %
               (project, tagging, revision, entitytype)
               )

    authors_to_artifacts = dbm.doFetchAll()
    return authors_to_artifacts


def get_artifacts_per_author(dbm, project, tagging, end_rev, artifact, range_resdir):
    """
    Selects the list of artifacts per developer for the given project, tagging, and release range, using the
    database-manager parameter. The kind of artifact is defined by the kind parameter. Afterwards, the pairs
    (author_name, artifact_name) are written to the file '[kind].list' in range_resdir.

    :param dbm: the database manager to use
    :param project: the project name to search
    :param tagging: the tagging analysis for the current project
    :param end_rev: the release tag defining the end of a release range
    :param artifact: the kind of artifact to search for
    :param range_resdir: the desired release range of the project
    """

    # get artifact information per author
    authors_to_artifacts = __select_artifacts_per_author(dbm, project, tagging, end_rev, artifact)

    # convert a2a to tuples (id, artifact)
    lines = ["{}; {}\n".format(dev_name, art) for dev_id, dev_name, art in authors_to_artifacts]

    # write lines to file for current kind of artifact (e.g., authors2feature, authors2function)
    outfile = pathjoin(range_resdir, "author2" + artifact.lower() + ".list")
    f = open(outfile, 'w')
    f.writelines(lines)
    f.close()
