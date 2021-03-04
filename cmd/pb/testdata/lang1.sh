#!/bin/bash -e

run() {
    cmd=$1
    shift
    case $cmd in
        say)
            echo "$@"
            ;;
        *)
            echo invalid command: $cmd
            exit 1
            ;;
    esac
}


code=$1

unset wanthash 
cat $code | while read line
do
    # skip blank lines
    if echo $line | egrep -q '^\s*$' 
    then
        continue
    fi

    # skip comment lines
    if echo $line | egrep -q '^\s*#' 
    then
        continue
    fi

    # our own hash is on first nonblank line
    if [ -z "$wanthash" ] 
    then
        wanthash="$line"
        havehash=sha256/$(sha256sum $0 | awk '{print $1}')
        if [ "$wanthash" != "$havehash" ]
        then
            echo "warning: interpreter hash out of date in $code: actual is $havehash" >&2
        fi
        continue
    fi

    # process statements
    run $line
done

