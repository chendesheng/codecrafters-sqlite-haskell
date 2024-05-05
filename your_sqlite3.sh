#!/bin/bash
#
# DON'T EDIT THIS!
#
# CodeCrafters uses this file to test your code. Don't make any changes here!
#
# DON'T EDIT THIS!

set -e
(
    cd $(dirname "$0")
    cabal install --overwrite-policy=always -v0
)
exec hs-sqlite-clone-exe "$@"