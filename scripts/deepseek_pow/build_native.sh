#!/usr/bin/env sh
set -eu
DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CC=${CC:-gcc}
"$CC" -O3 -march=native -pthread "$DIR/dspow_native.c" -o "$DIR/dspow_native.new"
chmod 755 "$DIR/dspow_native.new"
mv "$DIR/dspow_native.new" "$DIR/dspow_native"
echo "built: $DIR/dspow_native"
