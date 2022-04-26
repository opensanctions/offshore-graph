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
CALL apoc.periodic.iterate(
"MATCH (n) RETURN n",
"WITH n
UNWIND keys(n) as key
WITH n, key WHERE n[key] = ''
with n, collect(key) as propertyKeys limit 100
CALL apoc.create.removeProperties(n, propertyKeys)
yield node
RETURN count(*)",
{batchSize:10000, parallel:true}
);
