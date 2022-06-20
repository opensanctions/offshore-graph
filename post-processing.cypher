//This is meant to remove some information (eg super nodes) from the database to improve the investigation experience

//Remove "el portador"
MATCH (a:name) WHERE a.id =~ 'name:el-portador' DETACH DELETE a;

//Remove "internal user"
MATCH (a:name) WHERE a.id =~ 'name:internal-user' DETACH DELETE a;

//Remove "the bearer"
MATCH (a:name) WHERE a.id =~ 'name:the-bearer.*' DETACH DELETE a;


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


// Delete name nodes
CALL apoc.periodic.commit("MATCH (n:identifier) WHERE size((n)--()) <= 1 WITH n LIMIT $limit DETACH DELETE (n) RETURN COUNT(*)",{limit:10000, batchSize:10000, parallel:true});
CALL apoc.periodic.commit("MATCH (n:name) WHERE size((n)--()) <= 1 WITH n LIMIT $limit DETACH DELETE (n) RETURN COUNT(*)",{limit:10000, batchSize:10000, parallel:true});

MATCH (n:name) WHERE size((n)--()) <= 1 DETACH DELETE (n);
MATCH (n:email) WHERE size((n)--()) <= 1 DETACH DELETE (n);
MATCH (n:phone) WHERE size((n)--()) <= 1 DETACH DELETE (n);
MATCH (n:identifier) WHERE size((n)--()) <= 1 DETACH DELETE (n);
