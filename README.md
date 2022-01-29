# graph-demo
Loading OpenSanctions into Neo4J and Linkurious



### queries

MATCH (p:Politician),
      (o:Offshore),
      p = shortestPath((p)-[*]-(o))
WHERE length(p) > 1
RETURN p