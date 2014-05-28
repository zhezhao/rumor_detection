#! /bin/bash


if [ -z $1 ]; then
	echo "Lack of paratmer"
	exit 0
fi

if [ -z $2 ]; then
	echo "Demo mode, read past file using slowcat!"
	rumor_detection/slowcat $1 | rumor_detection/src/python/filter.py | rumor_detection/src/python/pipeline.py 0 1
else
	echo "Begin rumor detection with $2 buffer tweets!"
	if [ -z $3 ]; then
		useDB=0
	else
		useDB=$3
	fi
	if [ -z $4 ]; then
		useRank=1
	else
		useRank=$4
	fi
	tail -n $2 -f $1 | rumor_detection/src/python/filter.py | rumor_detection/src/python/pipeline.py $useDB $useRank
fi

#example usage on google cloud
#./rumor_detection/run.sh ./tstream 0 1
#example usage on demo server
#./rumor_detection/run.sh ../tstream
