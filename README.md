# CSCI636-project
CSCI636 - Big Data Analysis - Group Project<br>
Professor: Dr. Liu<br>
Team Member: Ge Ding, Hui(Henry) Chen, Jinghua Li

------

## Introduction
The dataset has a range of records, from 100, 000 to 1 billion. This data set consists of:<br>
* 100,000 ratings (1-5) from 943 users on 1682 movies. <br>
* Each user has rated at least 20 movies. <br>
* Simple demographic info for the users (age, gender, occupation, zip)<br>
  
There are 9 goals of data analysis on movie genres, movie ratings:<br>

1. Find the top ten popular most ratings movies of all time.
2. Find the top ten worst ratings movies of all time.
3. Find the top ten popular movies by year.(MapReduce)
4. Find out the top ten popular movies ratings of all time.
5. Find the top ten popular genres have the most ratings.
6. Find the top ten popular movies by year.(Spark)
7. Find the top ten popular movies in each genre.
8. Find the top ten worst movies in each genre.
9. Movie recommendation system

## Requirement
Python, Spark, Mapreduce

## Get Started
* Setup the datasource: ``` sh ./setup.sh ```

## Dataset Metadata
* http://files.grouplens.org/datasets/movielens/ml-100k-README.txt
