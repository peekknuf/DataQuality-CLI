#!/bin/bash

# Create root test directory
mkdir -p testdata

# Create 10 CSV files in the root directory
for i in {1..10}; do
  echo "id,name,value" >"testdata/file_$i.csv"
  echo "1,example,$RANDOM" >>"testdata/file_$i.csv"
done

# Create 3 subdirectories with 5 CSV files each
for dir in {1..3}; do
  mkdir -p "testdata/subdir_$dir"
  for i in {1..5}; do
    echo "id,name,value" >"testdata/subdir_$dir/file_$i.csv"
    echo "1,example,$RANDOM" >>"testdata/subdir_$dir/file_$i.csv"
  done
done

# Create a deeper nested directory
mkdir -p testdata/subdir_1/nested
for i in {1..3}; do
  echo "id,name,value" >"testdata/subdir_1/nested/file_$i.csv"
  echo "1,example,$RANDOM" >>"testdata/subdir_1/nested/file_$i.csv"
done

echo "Test data generated in ./testdata"
