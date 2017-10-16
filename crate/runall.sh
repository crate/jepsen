#! /bin/sh

for test in "dirty-read" "lost-updates" "version-divergence"
do
    lein run test --test ${test} --concurrency 2n --crate-version $1
    sleep 15
done
