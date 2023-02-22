#!/bin/bash

mkdir google-play-store-apps
chmod +rwx google-plays-store-apps
cd google-play-store-apps/
wget https://github.com/bdu-xpand-it/BDU-Recruitment-Challenges/blob/master/google-play-store-apps.zip?raw=true -O google-play-store-apps.zip
unzip google-play-store-apps.zip
rm google-play-store-apps.zip
cd ..
mvn clean package
spark-submit --class org.xpandit.sparkchallenge.App target/spark-challenge-1.0-SNAPSHOT.jar
