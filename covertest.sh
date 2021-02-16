#!/bin/bash -e

minpct=80
cmd="go test -v -timeout 20s -cover -coverprofile=/tmp/covertest.out -coverpkg=./..."

dirs=$(find -name go.mod |xargs dirname)

pass=true
for dir in $dirs
do
	cd $dir
	$cmd 
	html=/tmp/$(echo $PWD | perl -pne 's|^/||; s|/|-|g').html
	go tool cover -html=/tmp/covertest.out -o $html
	echo run this to see coverage detail: 
	echo xdg-open $html

	pct=$(go tool cover -func=/tmp/covertest.out | grep total: | perl -ne 'print if s/.*\s+(\d+)\..*/$1/')
	if test -z "$pct" 
	then
		echo FAIL unable to determine coverage 
		rm -f $html
		pass=false
	fi

	if test "0$pct" -lt "0$minpct"  
	then
		echo FAIL coverage less than $minpct
		pass=false
	fi
	cd -
done

if $pass
then
	echo PASS
else
	echo FAIL
	exit 1
fi
