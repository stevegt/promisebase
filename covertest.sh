#!/bin/bash 

minpct=80
cmd="go test -cover ./..."
tmp=/tmp/$$

$cmd > $tmp 2>&1 
pct=$(cat $tmp | perl -ne 'print if s/.*coverage:\s+(\d+)\..*/$1/')
if [ "$pct" -le "$minpct" ] 
then
	cat $tmp
	echo FAIL coverage less than $minpct
	exit 1
fi
