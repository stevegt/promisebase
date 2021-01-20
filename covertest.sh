#!/bin/bash 

minpct=80
cmd="go test -v -timeout 10s -cover -coverprofile=/tmp/covertest.out -coverpkg=./..."
tmp=/tmp/$$

$cmd 2>&1 | tee $tmp  
go tool cover -html=/tmp/covertest.out -o /tmp/covertest.html
echo run this to see coverage detail: 
echo xdg-open /tmp/covertest.html

pct=$(cat $tmp | grep coverage: | tail -1 | perl -ne 'print if s/.*coverage:\s+(\d+)\..*/$1/')
if test "$pct" -le "$minpct"  
then
	echo FAIL coverage less than $minpct
	exit 1
fi
