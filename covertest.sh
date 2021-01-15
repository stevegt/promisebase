#!/bin/bash 

minpct=80
cmd="go test -v -timeout 10s -cover ./..."
tmp=/tmp/$$

$cmd 2>&1 | tee $tmp  
pct=$(cat $tmp | tail -1 | perl -ne 'print if s/.*coverage:\s+(\d+)\..*/$1/m')
if test "$pct" -le "$minpct"  
then
	echo FAIL coverage less than $minpct
	exit 1
fi
