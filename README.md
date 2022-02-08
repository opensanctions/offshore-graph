# Sanctions/Offshores Graph Demo

This repository contains scripts that will merge the [OpenSanctions Due Diligence dataset](https://www.opensanctions.org/datasets/default/) with the [ICIJ OffshoreLeaks database](https://offshoreleaks.icij.org/) in order create a combined graph for analysis. 

The result is a [Cypher](https://neo4j.com/developer/cypher/) script to load the full graph into the [Neo4J database](https://neo4j.com/) and then browse it using the [Linkurious investigation platform](https://linkurious.com/investigation-platform/).

Based on name-based entity matching between the datasets, an analyst can use this graph to find offshore holdings linked to politically exposed and sanctioned individuals.

**This demo will be used in a joint Linkurious/OpenSanctions webinar on Feb. 24, 2022 (5pm CET). [Sign up here to participate](https://www.bigmarker.com/linkurious/Finding-evidence-of-corruption-and-money-laundering-with-open-data?utm_bmcr_source=OpenSanctions).**

## Import the data

Before loading the data into a fresh install of Neo4J (4.4), please make sure the database engine is configured to use enough heap memory for fast bulk imports. Add the following lines to your Neo4J server configuration file:

```ini
dbms.transaction.concurrent.maximum=0
dbms.memory.heap.max_size=8g
```

Once you have configured Neo4J, you can use the load script to import data from published CSV files:

* https://data.opensanctions.org/contrib/offshore-graph/exports/load.cypher

The simplest way to do this is to open the script in a text editor and copy and paste the contained commands into the Neo4J browser web UI.

After the data has loaded into Neo4J, connect your [Linkurious instance](https://doc.linkurio.us/admin-manual/latest/) to the instance. In the "Advanced" section of the "Global configuration", make sure to set ``indexationChunkSize`` to 500 (instead of the 5000 default value). You can also copy the contents of ``linkurious-captions.json`` and ``linkurious-styles.json`` into the relevant sections of the data source configuration dialog.

## Playing with the data

```cypher
MATCH (p:Politician),
      (o:Offshore),
      path = shortestPath((p)-[*]-(o))
RETURN path;
```

## License

The code in this repository is licensed under MIT terms, see ``LICENSE``. The OpenSanctions dataset is licensed under CC-BY-NonCommercial and free for media and NGO use. See the [project site](https://www.opensanctions.org/licensing/) for information about commercial licensing.