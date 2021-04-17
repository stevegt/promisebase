#!/usr/bin/env bash 

set -ex

pb=$PWD/cmd/pb/pb
cd cmd/pb
go build

basename=/tmp/stresstest
tmpdir=$basename.$$

# seed rng
#RANDOM=42

for i in {10..1}
do
	echo i is $i
	ulimit -v $(( $i * 100 * 1024 )) # kilobytes
	ulimit -v

	mkdir -p $tmpdir
	cd $tmpdir
	#size=$(( $RANDOM * $RANDOM / 1024 ))
    size=$(( 512 * 1024 ))

	$pb init 

    blobpath1=$(dd if=/dev/urandom bs=1024 count=$size | $pb putblob sha256)
	$pb getblob $blobpath1 > /dev/null

    blobpath2=$(dd if=/dev/urandom bs=1024 count=$size | $pb putblob sha256)
    treepath1=$($pb puttree sha256 $blobpath1 $blobpath2)

    # for j in {1..8000000}
    # do
        # XXX this will not work because we cant put 8 million
        # canpaths on the puttree command line

	# pb gettree <canpath>
	# pb linkstream <canpath> <name>
	# pb getstream <name>
	# pb lsstream [-a] <name>
	# pb catstream <name> [-o <filename>] 
	# pb cattree <canpath>
	# pb putstream [-q] <algo> <name>

	[ -n "$tmpdir" ]
    echo $tmpdir | grep $basename
	rm -rf $tmpdir
done



