VOTES_PATH = 54.csv
ROLLCALLS_PATH = 54-votacoes.csv

all: ${VOTES_PATH} ${ROLLCALLS_PATH}
	./bin/rice_index --input ${VOTES_PATH} --majority-percentual 0.975 --party ${PARTIES} --groupby party --metric adjusted_rice_index\
		| ./bin/rollmean\
		| ./bin/breakout_detection --metadata-csv-path ${ROLLCALLS_PATH} --plot-path output.png --plot-title "${PARTIES}"
	gnome-open output.png

names: ${VOTES_PATH} ${ROLLCALLS_PATH}
	./bin/rice_index --input oc-54.csv --majority-percentual 0.975 --name ${NAMES} --groupby name --metric adjusted_rice_index\
		| ./bin/rollmean\
		| ./bin/breakout_detection --metadata-csv-path oc-54-votacoes.csv --plot-path output.png --plot-title "${NAMES}"
	gnome-open output.png

${VOTES_PATH} ${ROLLCALLS_PATH}:
	./bin/votes_to_csv --legislature 54 --votes-output-path ${VOTES_PATH} --rollcalls-output-path ${ROLLCALLS_PATH}
