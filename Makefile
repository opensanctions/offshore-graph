
serve:
	cd data && python -m http.server 9999

data:
	mkdir -p data/

data/default.json: data
	wget -q -c -O data/default.json https://data.opensanctions.org/datasets/latest/default/entities.ftm.json

data/offshore.json: data
	wget -q -c -O data/offshore.json https://data.opensanctions.org/contrib/icij-offshoreleaks/full-oldb.json

get-data: data/default.json data/offshore.json

full: data/default.json data/offshore.json
	python export.py -p https://data.opensanctions.org/contrib/offshore-graph/exports data/default.json data/offshore.json