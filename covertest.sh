#!/bin/bash -e

minpct=70
cmd="go test -v -timeout 20s -cover -coverprofile=/tmp/covertest.out -coverpkg=./..."

dirs=$(find -name go.mod |xargs dirname)

passes=""
fails=""
for dir in $dirs
do
	pass=true
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
		fails="$fail $dir"
		pass=false
	fi
    echo coverage $pct%

	if test "0$pct" -lt "0$minpct"  
	then
		echo FAIL coverage less than $minpct
		fail="$fail $dir"
		pass=false
	fi
	if $pass
	then
		passes="$passes $dir"
	fi
	cd -
done

for pass in $passes
do
	echo PASS $pass
done

for fail in $fails
do
	echo FAIL $fail
done

if [ -n "$fails" ]
then
	exit 1
else
	echo PASS all functional and coverage tests
fi
