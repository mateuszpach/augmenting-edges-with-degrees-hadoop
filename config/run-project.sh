#!/bin/bash

# compile and run project

# go to project directory
cd /project

# remove old directories
hadoop fs -rm -r output
hadoop fs -rm -r input
hadoop fs -rm -r tmpdata

# create input directory on HDFS
hadoop fs -mkdir -p input

# generate big test
python3 ./gen.py 101 > input/file4.txt

# put input files to HDFS
hdfs dfs -put input/* input

# compile java project
hadoop com.sun.tools.javac.Main Project.java
jar cf project.jar Project*.class

# run project
hadoop jar project.jar Project input output tmpdata

# print the input files
echo -e "\ninput file1.txt:"
hdfs dfs -cat input/file1.txt

echo -e "\ninput file2.txt:"
hdfs dfs -cat input/file2.txt

# print the output of project
echo -e "\nproject output:"
# hdfs dfs -cat output/part-r-00000

