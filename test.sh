#!/usr/bin/env bash

for i in {0..6} ; do
    cargo run -- ./data/${i}.in.csv > ./target/${i}.run.csv
    diff ./data/${i}.out.csv ./target/${i}.run.csv
done