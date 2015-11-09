SET @proj = 'busybox';
SET @tagging = 'proximity';
SET @rev = '1_17_0';

SELECT el.fromId, el.toId, SUM(el.weight) as weight
#SELECT p.name, c.releaseRangeId, c.clusterMethod, l2.tag, el.fromId, p1.name as author1, el.toId, p2.name as author2, SUM(el.weight) as weight

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

# add cluster analysis
JOIN cluster c
ON r.id = c.releaseRangeId
# and corresponding edgelist
JOIN edgelist el
ON el.clusterId = c.id

# add authors/developers/persons
JOIN person p1
ON el.fromId = p1.id
JOIN person p2
ON el.toId = p2.id

# filter for current release range and artifact
WHERE p.name = CONCAT(@proj, "_", @tagging)
AND p.analysisMethod = @tagging
AND l2.tag = @rev
AND c.clusterMethod = "email"

GROUP BY p1.id, p2.id
ORDER BY p1.id ASC, p2.id ASC

# LIMIT 10
