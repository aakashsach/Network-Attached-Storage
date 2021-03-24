#!/bin/sh

if [ "$#" -eq 0 ]; then
	set -x
	PYTHONPATH=$PWD/lib:$PWD python -m unittest discover tests -v
else
	set -x
	PYTHONPATH=$PWD/lib:$PWD python -m unittest -v $*
fi
