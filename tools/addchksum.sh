#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CURDIR="$PWD"

for tree in aosc-os-core aosc-os-abbs aosc-os-arm-bsps; do
    pushd "$tree"
    find . -maxdepth 3 -type f -name spec | \
        parallel -j4 --bar "python3 $DIR/addchksum.py {} 2>$CURDIR/chksumerr.log"
    popd
done
