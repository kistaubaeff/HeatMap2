#!/bin/bash

rm -rf input
mkdir input

for i in {1..200}; do
  x=$((RANDOM % 1000))
  y=$((RANDOM % 1500))
  userId=$((RANDOM % 1000))
  timestamp=$((RANDOM % 1000000000))
  echo "$x $y $userId $timestamp" >> input/clicks.1

done

for i in {1..200}; do
  x=$((RANDOM % 1000))
  y=$((RANDOM % 1500))
  userId=$((RANDOM % 1000))
  timestamp=$((RANDOM % 1000000000))
  echo "$x $y $userId $timestamp" >> input/clicks.2

done

for i in {1..200}; do
  x=$((RANDOM % 1000))
  y=$((RANDOM % 1500))
  userId=$((RANDOM % 1000))
  timestamp=$((RANDOM % 1000000000))
  echo "$x $y $userId $timestamp" >> input/clicks.3

done

