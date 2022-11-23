STORE=sqlite:///data/followthemoney.store

.PHONY: dedupe serve dev

serve:
	cd data && python -m http.server 9999

data:
	mkdir -p data/

data/graph.json: data
	wget -q -c -O data/graph.json https://data.opensanctions.org/contrib/graph/graph.json

data/opensanctions.json: data
	wget -q -c -O data/opensanctions.json https://data.opensanctions.org/datasets/latest/default/entities.ftm.json

data/dedupe:
	mkdir -p data/dedupe

data/dedupe/%.head.csv:
	head -1 data/exports/$*.csv >$@

data/dedupe/%.body.csv:
	tail -n +2 data/exports/$*.csv >$@

data/dedupe/%.uniq.csv: data/dedupe/%.body.csv
	sort -u data/dedupe/$*.body.csv -o data/dedupe/$*.uniq.csv

data/exports/%.csv: data/dedupe/%.head.csv data/dedupe/%.uniq.csv
	cat data/dedupe/$*.head.csv data/dedupe/$*.uniq.csv >data/exports/$*.csv

dedupe:
	rm -rf data/dedupe
	mkdir -p data/dedupe
	make -B data/exports/*.csv

full: data/graph.json
	python export.py -p https://data.opensanctions.org/contrib/offshore-graph/exports data/graph.json

convert-dev: data/opensanctions.json 
	python export.py -p http://localhost:9999/exports data/opensanctions.json

dev: convert-dev dedupe

clean:
	rm -f data/graph.json
	rm -f data/opensanctions.json
	rm -rf data/dedupe
	rm -rf data/exports
