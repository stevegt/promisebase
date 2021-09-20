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


script_key=$1

# If `pb` is the API an external process uses to use to talk to the
# database, then we need to run `pb` here.  But while we're testing
# `pb`, we can't assume that it's built, so instead we just `go run`.
exec 7< <(../pb cattree $script_key)

unset wanthash 
while read line <&7
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
        continue
    fi

    # process statements
    run $line
done

wait 
exit $?
