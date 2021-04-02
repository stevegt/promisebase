#!/usr/bin/env bash 

set -e

minpct=70
cmd="go test -v -timeout 60s -cover -coverprofile=/tmp/covertest.out -coverpkg=./..."

# dirs=$(find -name go.mod |xargs dirname)
dirs=". cmd/pb" # hardcoded so we can control sequence

declare -A msg

for dir in $dirs
do
	cd $dir
	msg[$dir]="FAIL"
	if $cmd 
	then 
		msg[$dir]="PASS"

		html=/tmp/$(echo $PWD | perl -pne 's|^/||; s|/|-|g').html
		go tool cover -html=/tmp/covertest.out -o $html
		echo run this to see coverage detail: 
		echo xdg-open $html

		pct=$(go tool cover -func=/tmp/covertest.out | grep total: | perl -ne 'print if s/.*\s+(\d+)\..*/$1/')
		if [ "0$pct" -le "0" ]
		then
			msg[$dir]="FAIL unable to determine coverage"
			rm -f $html
		elif test "0$pct" -lt "0$minpct"  
		then
			msg[$dir]="FAIL coverage $pct is less than $minpct"
		else
			echo coverage $pct%
		fi
	else
		exit 1
	fi

	# echo ${msg[$dir]}
	cd -
done

if ! which errcheck
then
	echo recommend you install errcheck:
	echo go get -u github.com/kisielk/errcheck
else
	echo looking for unchecked errors:
	errcheck . || true
	cd cmd/pb; errcheck . || true
fi

echo 
echo Summary of all tests:
rc=0
for dir in $dirs
do
	# printf "%40s %s\n" $(realpath $dir) "${msg[$dir]}"
	printf "%-10s %s\n" $dir "${msg[$dir]}"
	if echo ${msg[$dir]} | grep -q FAIL 
	then
		rc=1
	fi
done

# echo ${msg[@]}

if [ "$rc" -ne 0 ]
then
	exit $rc
else
	echo PASS all functional and coverage tests
fi
