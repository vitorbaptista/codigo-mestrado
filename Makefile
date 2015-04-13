LEGISLATURE = 54
VOTES_PATH = ${LEGISLATURE}.csv
ROLLCALLS_PATH = ${LEGISLATURE}-votacoes.csv

all: ${VOTES_PATH} ${ROLLCALLS_PATH}
	./bin/rice_index --input ${VOTES_PATH} --majority-percentual 0.975 --party ${PARTIES} --groupby party --metric adjusted_rice_index\
		| ./bin/rollmean\
		| ./bin/breakout_detection --metadata-csv-path ${ROLLCALLS_PATH} --plot-path output.png --plot-title "${PARTIES} (${LEGISLATURE} legislatura)"
	gnome-open output.png

names: ${VOTES_PATH} ${ROLLCALLS_PATH}
	./bin/rice_index --input ${VOTES_PATH} --majority-percentual 0.975 --name ${NAMES} --groupby name --metric adjusted_rice_index\
		| ./bin/rollmean\
		| ./bin/breakout_detection --metadata-csv-path ${ROLLCALLS_PATH} --plot-path output.png --plot-title "${NAMES} (${LEGISLATURE} legislatura)"
	gnome-open output.png

%.csv %-votacoes.csv:
	./bin/votes_to_csv --legislature $* --votes-output-path $*.csv --rollcalls-output-path $*-votacoes.csv
