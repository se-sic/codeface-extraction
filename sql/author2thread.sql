SET @proj = 'busybox';
SET @tagging = 'feature';
SET @rev = '1_20_0';

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
WHERE p.name = CONCAT(@proj, "_", @tagging)
AND p.analysisMethod = @tagging
AND l2.tag = @rev
AND m.creationDate BETWEEN l1.date AND l2.date

ORDER BY m.threadId, pers.name ASC

#LIMIT 7000
