#!/usr/bin/env bash

time=$(date +%d%H%M)
nohup python3 main.py > log/log_$time.txt &
echo "pid = $!, log file = log_$time.txt"
