SET @proj = 'busybox';
SET @tagging = 'feature';
SET @rev = '1_17_0';
SET @entityType = 'Feature';


SELECT pers.id AS id, pers.name AS name

FROM project p

# add authors/developers/persons
JOIN person pers
ON p.id = pers.projectId

# filter for current release range and artifact
WHERE p.name = CONCAT(@proj, "_", @tagging)

# LIMIT 10