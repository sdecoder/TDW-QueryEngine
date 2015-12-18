#!/bin/bash

if [ x"$TDWPL_HOME" == "x" ]; then
    TDWPL_HOME=..
fi

if [ x"$TDWPL_THREADS" == "x" ]; then
    TDWPL_THREADS=2
fi

if [ x"$1" == "x" ]; then
    echo "Usage: $0 input_file output_file run/check"
    exit
elif [ x"$2" == "x" ]; then
    echo "Usage: $0 input_file output_file run/check"
    exit
elif [ x"$3" == "x" ]; then
    echo "Usage: $0 input_file output_file run/check"
    exit
fi

TDWPL=${TDWPL_HOME}/tdwpl

NO_TEST_SET="tdwpl_case2 tdwpl_case3"

function do_run() {
    for x in $NO_TEST_SET; do
        if [ x"$1" == x"$x.ut" ]; then
            return
        fi
    done
    i=0
    while [ $i -lt ${TDWPL_THREADS} ];
      do
      cat $1 | $TDWPL -atestshell > $i.$2 &
      let i+=1
    done
}

function do_check() {
    for x in $NO_TEST_SET; do
        if [ x"$1" == x"$x" ]; then
            return
        fi
    done
    i=0
    while [ $i -lt ${TDWPL_THREADS} ];
      do
      sed -e '/^Checking/d;/.*server.*/d;/^Module/d;/.*Session/d;/.*File/d;' $i.$1.mres > $1.1
      sed -e '/^Checking/d;/.*server.*/d;/^Module/d;/.*Session/d;/.*File/d;' $1.std > $1.2
      diff -Nur -s $1.1 $1.2
      rm -rf $1.1 $1.2
      let i+=1
    done
}

if [ x"$3" == "xrun" ]; then
    do_run $1 $2
elif [ x"$3" == "xcheck" ]; then
    do_check $1
fi
