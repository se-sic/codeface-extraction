SET @proj = 'busybox';
SET @tagging = 'feature';
SET @rev = '1_20_0';
SET @entityType = 'Feature';

SELECT pers.id AS id, pers.name AS name, cd.entityId AS artifact
#SELECT c.commitHash, c.commitDate, cd.file, cd.entityId AS function, pers.name AS name

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
WHERE p.name = CONCAT(@proj, "_", @tagging)
AND p.analysisMethod = @tagging
AND l2.tag = @rev
AND cd.entityType = @entityType

#GROUP BY cd.entityId ASC
ORDER BY cd.entityId, pers.name ASC

#LIMIT 7000