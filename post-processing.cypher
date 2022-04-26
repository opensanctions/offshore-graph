//This is meant to remove some information (eg super nodes) from the database to improve the investigation experience

//Remove "el portador"
MATCH (a)
WHERE ID(a) = 1377854
DETACH DELETE a;

//Remove "internal user"
MATCH (a)
WHERE ID(a) = 2626854
DETACH DELETE a;

//Remove "the bearer"
MATCH (a)
WHERE ID(a) = 1377286
DETACH DELETE a;

//Delete blank properties
CALL apoc.periodic.commit(
"MATCH (n)
UNWIND keys(n) as k
WITH n, k 
WHERE n[k] = ''
WITH n, collect(k) as propertyKeys LIMIT $limit
CALL apoc.create.removeProperties(n, propertyKeys)
yield node
RETURN count(*)",{limit:10000});
