#!/usr/bin/env bash 

set -ex

pb=$PWD/cmd/pb/pb
cd cmd/pb
go build

tmpdir=/tmp/stresstest.$$

# seed rng
#RANDOM=42

for i in {22..1}
do
	echo i is $i
	ulimit -v $(( $i * 100 * 1024 )) # kilobytes
	ulimit -v

	mkdir -p $tmpdir
	cd $tmpdir
	#size=$(( $RANDOM * $RANDOM / 1024 ))
    size=$(( 1024 * 1024 ))

	$pb init 

    canpath=$(dd if=/dev/urandom bs=1024 count=$size | $pb putblob sha256)
	$pb getblob $canpath > /dev/null

	# pb puttree <algo> <canpaths>... 
	# pb gettree <canpath>
	# pb linkstream <canpath> <name>
	# pb getstream <name>
	# pb lsstream [-a] <name>
	# pb catstream <name> [-o <filename>] 
	# pb cattree <canpath>
	# pb putstream [-q] <algo> <name>

	[ -n "$tmpdir" ]
	rm -rf $tmpdir
done



