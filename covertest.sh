#!/usr/bin/env bash 

set -e

unset DBDIR

minpct=70
cmd="go test -v -timeout 60s -cover -coverprofile=/tmp/covertest.out -coverpkg=./..."

# dirs=$(find -name go.mod |xargs dirname)
dirs=". cmd/pb" # hardcoded so we can control sequence

declare -A gotest cover errcheck golint

for dir in $dirs
do
	cd $dir
	gotest[$dir]=FAIL
	cover[$dir]=FAIL
	errcheck[$dir]=FAIL
	golint[$dir]=FAIL
	if $cmd 
	then 
		gotest[$dir]=PASS

		html=/tmp/$(echo $PWD | perl -pne 's|^/||; s|/|-|g').html
		go tool cover -html=/tmp/covertest.out -o $html

		pct=$(go tool cover -func=/tmp/covertest.out | grep total: | perl -ne 'print if s/.*\s+(\d+)\..*/$1/')
		if [ "0$pct" -le "0" ]
		then
			echo "FAIL unable to determine coverage"
			rm -f $html
		elif test "0$pct" -lt "0$minpct"  
		then
			echo "FAIL coverage $pct is less than $minpct"
		else
			echo coverage $pct%
			cover[$dir]=PASS
		fi
		echo run this to see coverage detail: 
		echo xdg-open $html
	fi

	if ! which errcheck
	then
		echo recommend you install errcheck:
		echo go get -u github.com/kisielk/errcheck
	else
		echo looking for unchecked errors:
		if errcheck . 
		then
			errcheck[$dir]=PASS
		fi
	fi

	if golint -set_exit_status
	then
		golint=PASS
	fi

	cd -
done

echo 
echo Summary of all tests:
for dir in $dirs
do
	printf "%-15s " $dir
	printf "gotest %s "   "${gotest[$dir]}"
	printf "cover %s "    "${cover[$dir]}"
	printf "errcheck %s " "${errcheck[$dir]}"
	printf "golint %s "   "${golint[$dir]}"
	printf "\n"
done

if echo "${gotest[@]} ${cover[@]} ${errcheck[@]} ${golint[@]}" | grep -q FAIL 
then
	exit 1
else
	echo PASS all tests
fi

