#! /bin/sh

lein run test --test dirty-read --concurrency 30 --crate-version $1 --time-limit 100
