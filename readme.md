# Library Synopsis
The objective of this library was to experiment with different processing methods for my data being collected by my
data.

### The Problem
Up to this point my options data pulling tool has been running on a heroku webserver and pushing to a MongoDB server. I 
chose these tools because I learned how to use them in University and I figured I would rather have poorly formatted and
stored data than no data at all. **The main issue** arises when I want to work with this data extremely quickly for
machine learning purposes.

See with the data being stored in MongoDB I only know how to pull it off in a JSON format. This being a flat-database it
doesn't really work well with the quick indexing I will need for data processing and actual machine learning application.
 
 
 
 # Change Log
 
 - 11/10/2020
    - Decided to move data to kaggle in a .csv format. This could free my computer from having to host a sql database 
    (as fun as thats been! /s)
    
 # To-Do
 - Make sure that price does not play a role. I want this model to predict option price based on every other factor other than what stock symbol the option is for. Have the stock symbol merely be a multiplier. 
 - The goal is to make sure this model isn't overfit to the data. It's easy to randomly pull a selection from a certain regime of volatility but I want this to hold up outside of this regime.
