#! /bin/bash


if [ -z $1 ]; then
	echo "Lack of paratmer"
	exit 0
fi

tail -n 100000 -f $1 | rumor_detection/src/python/filter.py | rumor_detection/src/python/pipeline.py $2

