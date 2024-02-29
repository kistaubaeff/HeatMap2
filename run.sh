#!/bin/bash

mvn clean package

./generateClicks.sh

hdfs dfs -rm -r /output

hdfs dfs -rm -r /input

hdfs dfs -put input /input

hdfs dfs -put input_handbooks /input_handbooks

yarn jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar /input /output
