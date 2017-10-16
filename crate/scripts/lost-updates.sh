#! /bin/sh

lein run test --test lost-updates --concurrency 2n --crate-version $1
