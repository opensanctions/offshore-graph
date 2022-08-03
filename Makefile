STORE=sqlite:///data/followthemoney.store

serve:
	cd data && python -m http.server 9999

data:
	mkdir -p data/

data/resolv.ijson: data
	wget -q -c -O data/resolv.ijson https://github.com/opensanctions/opensanctions/raw/main/opensanctions/static/resolve.ijson

data/default.json: data
	wget -q -c -O data/default.json https://data.opensanctions.org/datasets/latest/default/entities.ftm.json

data/default.resolved.json: data/default.json data/resolv.ijson
	nomenklatura apply -o data/default.resolved.json -r data/resolv.ijson data/default.json

data/offshore.json: data
	wget -q -c -O data/offshore.json https://data.opensanctions.org/contrib/icij-offshoreleaks/full-oldb.json

data/offshore.resolved.json: data/offshore.json data/resolv.ijson
	nomenklatura apply -o data/offshore.resolved.json -r data/resolv.ijson data/offshore.json

data/gleif.json: data
	wget -q -c -O data/gleif.json https://data.opensanctions.org/contrib/gleif/gleif.json

data/gleif.resolved.json: data/gleif.json data/resolv.ijson
	nomenklatura apply -o data/gleif.resolved.json -r data/resolv.ijson data/gleif.json

data/rupep.json: data
	wget -q -c -O data/rupep.json https://data.opensanctions.org/datasets/latest/ru_rupep_companies/entities.ftm.json

data/rupep.resolved.json: data/rupep.json data/resolv.ijson
	nomenklatura apply -o data/rupep.resolved.json -r data/resolv.ijson data/rupep.json

data/combined.json: data/offshore.resolved.json data/gleif.resolved.json data/default.resolved.json data/rupep.resolved.json
	sort -o data/combined.dupes.json data/offshore.resolved.json data/gleif.resolved.json data/default.resolved.json data/rupep.resolved.json
	nomenklatura sorted-merge -o data/combined.json data/combined.dupes.json

# ftm store write --db $(STORE) -d combined -i data/default.json
# ftm store write --db $(STORE) -d combined -i data/offshore.resolved.json
# ftm store write --db $(STORE) -d combined -i data/gleif.resolved.json
# ftm store write --db $(STORE) -d combined -i data/rupep.resolved.json
# ftm store iterate --db $(STORE) -d combined -o data/combined.json

get-data: data/default.json data/offshore.json data/gleif.json

full: data/combined.json
	python export.py -p https://data.opensanctions.org/contrib/offshore-graph/exports data/combined.json

dev: data/combined.json
	python export.py -p http://localhost:9999/exports data/combined.json

clean:
	rm -f data/resolv.ijson
	rm -f data/offshore.resolved.json
	rm -f data/offshore.json
	rm -f data/gleif.resolved.json
	rm -f data/gleif.json
	rm -f data/default.resolved.json
	rm -f data/default.json
	rm -f data/combined.dupes.json
	rm -f data/combined.json
