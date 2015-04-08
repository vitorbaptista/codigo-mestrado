all:
	./bin/rice_index --input oc-54.csv --majority-percentual 0.975 --party ${PARTIES} --groupby party\
		| ./bin/rollmean\
		| ./bin/breakout_detection --metadata-csv-path oc-54-votacoes.csv --plot-path output.png --plot-title "${PARTIES}"
	gnome-open output.png
