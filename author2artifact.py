from logging import getLogger

log = getLogger(__name__)
from os.path import join as pathjoin, exists as pathexists
from os import makedirs

from codeface.dbmanager import DBManager
from codeface.configuration import Configuration


def __select_list_of_authors(dbm, project):
    dbm.doExec("""
                    SELECT pers.id AS id, pers.name AS name

                    FROM project p

                    # add authors/developers/persons
                    JOIN person pers
                    ON p.id = pers.projectId

                    # filter for current release range and artifact
                    WHERE p.name = %s

                    ORDER BY name ASC

                    # LIMIT 10
               """,
               project
               )

    list_of_authors = dbm.doFetchAll()
    return list_of_authors


def get_list_of_authors(dbm, project, range_resdir):
    # get authors for given project
    list_of_authors = __select_list_of_authors(dbm, project)
    # convert to a proper list for file writing
    lines = ["{}; {}\n".format(dev_id, dev_name) for dev_id, dev_name in list_of_authors]
    # write lines to file
    outfile = pathjoin(range_resdir, "authors.list")
    f = open(outfile, 'w')
    f.writelines(lines)
    f.close()


# FIXME add feature expression to Codeface database!


def __select_artifacts_per_author(dbm, project, tagging, revision, entityType="FEATURE"):
    dbm.doExec("""
                    SELECT pers.id AS id, pers.name AS name, cd.entityId AS artifact

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

                    # add commits for the ranges
                    JOIN commit c
                    on r.id = c.releaseRangeId

                    # add meta-data for commits
                    JOIN commit_dependency cd
                    ON c.id = cd.commitId

                    # add authors/developers/persons
                    JOIN person pers
                    ON c.author = pers.id

                    # filter for current release range and artifact
                    WHERE p.name = %s
                    AND p.analysisMethod = %s
                    AND l2.tag = %s
                    AND cd.entityType = %s

                    GROUP BY name, artifact ASC
                    ORDER BY id, artifact ASC

                    # LIMIT 10
                """,
               (project, tagging, revision, entityType)
               )

    authors_to_artifacts = dbm.doFetchAll()
    return authors_to_artifacts


def get_artifacts_per_author(dbm, project, tagging, kind, end_rev, artifact, range_resdir):
    # get artifact information per author
    authors_to_artifacts = __select_artifacts_per_author(dbm, project, tagging, end_rev, artifact)
    # convert a2a to tuples (id, artifact)
    lines = ["{}; {}\n".format(dev_name, art) for dev_id, dev_name, art in authors_to_artifacts]
    # write lines to file for current kind of artifact (e.g., authors2feature, authors2function)
    outfile = pathjoin(range_resdir, kind + ".list")
    f = open(outfile, 'w')
    f.writelines(lines)
    f.close()


##
# RUN FOR ALL PROJECTS
##

def run_extraction(systems, artifact2tagging, codeface_conf, project_conf, resdir):
    # for all projects
    for current_system in systems:

        # for all kinds of artifacts
        for kind, (artifact, tagging) in artifact2tagging.iteritems():

            # load configuration and initialize database manager
            conf = Configuration.load(codeface_conf, project_conf.format(current_system, tagging))
            dbm = DBManager(conf)

            # get setting for current combination
            project = conf["project"]
            revs = conf["revisions"]
            project_resdir = pathjoin(resdir, current_system, tagging)

            # FIXME check if project exists in database, continue otherwise
            # if dbm.cur.rowcount == 0:
            #     # Project is not contained in the database
            #     raise Exception("Project {}/{} does not exist in database!".
            #                     format(project, tagging))

            # for all revisions of this project
            for i in range(len(revs) - 1):
                start_rev = revs[i]
                end_rev = revs[i + 1]

                print (project, tagging, kind, start_rev, end_rev)

                # results directory for current revision
                range_resdir = pathjoin(project_resdir, "{0}-{1}".format(start_rev, end_rev))
                if not pathexists(range_resdir):
                    makedirs(range_resdir)

                get_artifacts_per_author(dbm, project, tagging, kind, end_rev, artifact, range_resdir)

                get_list_of_authors(dbm, project, range_resdir)


if __name__ == '__main__':
    ##
    # CONSTANTS
    ##

    __systems = ["busybox", "linux", "openssl", "sqlite", "tcl"]
    #  FIXME run all analyses again. completely.

    __artifact2tagging = {
        'author2feature': ("Feature", 'feature'),
        'author2function': ("Function", 'proximity')  # ,
        # 'author2file':  ("file", "proximity")  # FIXME implement author2file (needs new SELECT)
    }

    ##
    # CONSTRUCT PATHS
    ##

    __cf_vm = "/local/hunsen/codeface"
    __cf_dir = pathjoin(__cf_vm, "codeface-repo")

    __resdir = pathjoin(__cf_vm, "results")
    __codeface_conf = pathjoin(__cf_dir, "codeface-test-a2a.conf")
    __project_conf = pathjoin(__cf_dir, "conf/spl/{}_{}.conf")

    run_extraction(__systems, __artifact2tagging, __codeface_conf, __project_conf, __resdir)
