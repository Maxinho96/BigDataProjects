#!/bin/bash

INPUT=$1
OUTPUT=$2
N=$3

for ((i=0; i<$N; i++))
do
	cat $INPUT >> $OUTPUT;
done
