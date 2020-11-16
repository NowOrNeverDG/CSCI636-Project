#!/bin/bash

# install Java, Scala and Git
sudo apt install default-jdk scala git -y

# download Apache Spark
wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz

# unzip the spark file to the current directory
tar xvf spark-*

# move the unzipped spark file to "opt/spark"
sudo mv spark-3.0.1-bin-hadoop2.7 /opt/spark

# add the spark to the envir variable
echo "export SPARK_HOME=/opt/spark" >> ~/.profile
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile
# Note, you need to specify which python you wanna use

# refresh the profile shell
source ~/.profile

echo "You have successfully set up spark in your Linux system!"
