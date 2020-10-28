#!/bin/sh

# make a dir if haven't #
DIR="./dataset"
FILE = ml-100k.zip

if [ -d "$DIR" ]; then
  cd $DIR
  
  # check if the dataset is downloaded #
  if test ! -f "$FILE"; then
    wget http://files.grouplens.org/datasets/movielens/ml-100k.zip
  fi

else
  mkdir dataset
  cd dataset
  wget http://files.grouplens.org/datasets/movielens/ml-100k.zip
fi

for file in ml-100k.zip; do
  unzip "${file}"
done