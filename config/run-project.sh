#!/bin/bash

# compile and run project
export JAVA_HOME=/usr/java/default
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

# go to project directory
cd /project

# create input directory on HDFS
hadoop fs -mkdir -p input

# put input files to HDFS
hdfs dfs -put input/* input

# compile java project
hadoop com.sun.tools.javac.Main Project.java
jar cf project.jar Project*.class

# run project
hadoop jar project.jar Project input output

# print the input files
echo -e "\ninput file1.txt:"
hdfs dfs -cat input/file1.txt

echo -e "\ninput file2.txt:"
hdfs dfs -cat input/file2.txt

# print the output of project
echo -e "\nproject output:"
hdfs dfs -cat output/part-r-00000

