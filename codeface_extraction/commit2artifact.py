# coding=utf-8
"""
This file provides the needed functions for the artifacts-commit extraction.
"""

from os.path import join as pathjoin


def __select_artifacts_per_commit(dbm, project, tagging, revision, entitytype="FEATURE"):
    dbm.doExec("""
                    SELECT c.id, cd.entityId AS artifact

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

                    # filter for current release range and artifact
                    WHERE p.name = '%s'
                    AND p.analysisMethod = '%s'
                    AND l2.tag = '%s'
                    AND cd.entityType = '%s'

                    ORDER BY c.id, artifact

                    # LIMIT 10
                """ %
               (project, tagging, revision, entitytype)
               )

    authors_to_artifacts = dbm.doFetchAll()
    return authors_to_artifacts


def get_cochanged_artifacts(dbm, project, tagging, end_rev, artifact, range_resdir):
    """
    Selects the list of touched artifacts per commit for the given project, tagging, and release range, using the
    database-manager parameter. Afterwards, the sets are written
     to the file 'cochanged-artifacts.list' in range_resdir.

    :param dbm: the database manager to use
    :param project: the project name to search
    :param tagging: the tagging analysis for the current project
    :param end_rev: the release tag defining the end of a release range
    :param artifact: the kind of artifact to search for
    :param range_resdir: the desired release range of the project
    """

    # get list of changed artifacts per author
    commit2artifact = __select_artifacts_per_commit(dbm, project, tagging, end_rev, artifact)

    # convert c2a to tuples (commit, artifact)
    lines = ["{}; {}\n".format(commit_id, art) for commit_id, art in commit2artifact]

    # write lines to file for current kind of artifact
    outfile = pathjoin(range_resdir, "commit2" + artifact.lower() + ".list")
    f = open(outfile, 'w')
    f.writelines(lines)
    f.close()
