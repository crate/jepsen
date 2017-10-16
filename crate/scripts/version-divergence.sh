#! /bin/sh

lein run test --test version-divergence --concurrency 2n --crate-version $1
