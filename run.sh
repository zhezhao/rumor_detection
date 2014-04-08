#! /bin/bash


if [ -z $1 ]; then
	echo "Lack of paratmer"
	exit 0
fi

tail -f $1 | src/python/filter.py | src/python/cluster.py

