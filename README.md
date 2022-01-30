# graph-demo
Loading OpenSanctions into Neo4J and Linkurious

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