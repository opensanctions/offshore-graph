# graph-demo

Loading OpenSanctions into Neo4J and Linkurious

Load script: https://data.opensanctions.org/contrib/offshore-graph/exports/load.cypher

## settings

```
dbms.transaction.concurrent.maximum = 0
dbms.memory.heap.max_size=8g
```

### queries

MATCH (p:Politician),
      (o:Offshore),
      path = shortestPath((p)-[*]-(o))
RETURN path;