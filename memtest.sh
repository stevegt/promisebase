#!/usr/bin/env bash 

# set -x

pb=$PWD/cmd/pb/pb
cd cmd/pb
go build

basename=/tmp/stresstest
tmpdir=$basename.$$

calc() 
{ 
    echo "$1" | bc
}

compare()
{
    [ $(calc "$1") -eq 1 ]
}

limit=$(calc "1 * 1024^3")
delta=$(calc "$limit * .5")

while true
do
    echo limit $(calc "$limit / 1024^2") megabytes

    mkdir -p $tmpdir
    cd $tmpdir

    (
	set -e
    set -x

	ulimit -v $(calc "$limit / 1024") # ulimit uses kilobytes
	ulimit -v

	$pb init 

	size=$(calc "100 * 1024^2")
	count=$(calc "$size / 1024")

	blockpath1=$(dd if=/dev/urandom bs=1024 count=$count | $pb putblock sha256)
	$pb getblock $blockpath1 > /dev/null 
	blockpath2=$(dd if=/dev/urandom bs=1024 count=$count | $pb putblock sha256)
	treepath1=$($pb puttree sha256 $blockpath1 $blockpath2)

	# for j in {1..8000000}
	# do
	# XXX this will not work because we can't put 8 million
	# canpaths on the puttree command line

	$pb gettree $treepath1 > /dev/null
	$pb linkstream $treepath1 gerald 
	$pb getstream gerald > /dev/null
	$pb lsstream -a gerald > /dev/null
	$pb catstream gerald > /dev/null 
	$pb cattree $treepath1 > /dev/null
	dd if=/dev/urandom bs=1024 count=$count | $pb putstream sha256 harold
    )
    rc=$?

    cd -
    [ -n "$tmpdir" ]
    echo $tmpdir | grep $basename
    rm -rf $tmpdir

    absdelta=$(calc "sqrt($delta^2)")
    if [ $rc -ne 0 ] && compare "$absdelta < 50*1024*1024"
    then
	break
    fi

    if [ $rc -eq 0 ] 
    then
	echo limit $(calc "$limit / 1024^2") megabytes PASS
	# need smaller limit
	# if delta is positive then reverse else continue
	compare "$delta > 0" && error=-.5 || error=1
    else
	echo limit $(calc "$limit / 1024^2") megabytes FAIL
	# need larger limit
	# if delta is negative then reverse else continue
	compare "$delta < 0" && error=-.5 || error=1
    fi
    echo =============================================
    delta=$(calc "$delta * $error")
    limit=$(calc "$limit + $delta")

    sleep 1
done

echo fails at virtual memory ulimit $(calc "$limit / (1024 * 1024)") megabytes


