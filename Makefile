STORE=sqlite:///data/followthemoney.store

serve:
	cd data && python -m http.server 9999

data:
	mkdir -p data/

data/graph.json: data
	wget -q -c -O data/graph.json https://data.opensanctions.org/contrib/graph/graph.json

data/opensanctions.json: data
	wget -q -c -O data/opensanctions.json https://data.opensanctions.org/datasets/latest/default/entities.ftm.json

full: data/graph.json
	python export.py -p https://data.opensanctions.org/contrib/offshore-graph/exports data/graph.json

dev: data/opensanctions.json
	python export.py -p http://localhost:9999/exports data/opensanctions.json

clean:
	rm -f data/graph.json
	rm -f data/opensanctions.json
