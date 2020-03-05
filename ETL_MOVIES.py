import pandas as pd
import json
from pymongo import MongoClient
def CleanData(rawImdbDF, rawRottenDF):
    imdbTrimDF = rawImdbDF[['title','year', 'imdbRating', 'ratingCount']]
    rottenTrimDF = rawRottenDF[['movie_title','in_theaters_date', 'audience_rating','audience_count']]
    imdbTrimDF.dropna(inplace=True)
    rottenTrimDF.dropna(inplace=True)
    imdbTrimDF['title'] = imdbTrimDF['title'].str.split('(').str[0]
    rottenTrimDF['in_theaters_date'] = rottenTrimDF['in_theaters_date'].str[0:4]
    rottenTrimDF['in_theaters_date'] = rottenTrimDF['in_theaters_date'].astype(int)
    rottenTrimDF['audience_rating'] = rottenTrimDF['audience_rating'].astype(float)
    rottenTrimDF['audience_rating'] = rottenTrimDF['audience_rating'].div(10)
    rottenTrimDF['audience_count'] = rottenTrimDF['audience_count'].astype(int)
    return imdbTrimDF, rottenTrimDF
def LoadCsv():
    #build file location string to read data
    imdbFile = "Resources/Sources/imdb.csv"
    rottenFile = "Resources/Sources/rotten_tomatoes_movies.csv"
    #load movie csv to data frames
    imdbDF = pd.read_csv(imdbFile, error_bad_lines=False)
    rottenDF = pd.read_csv(rottenFile, error_bad_lines=False)
    return imdbDF, rottenDF
def MongoDBInit():
    #Create Connection to Mongo DB and create the DB
    client = MongoClient(port=27017)
    db = client.MovieAnalysisDB
    return db
def MongoCollection(mongoDB):

    imdbCollection = mongoDB['IMDB']
    rottenCollection = mongoDB['Rotten']
    return imdbCollection, rottenCollection
def MongoInsert(imdbCol, rottenCol, cleanImdbDF, cleanRottenDF):
    result = cleanImdbDF.to_json()
    imdbDict = json.loads(result)
    imdbCol.insert_one(imdbDict)
    result = cleanRottenDF.to_json()
    rottenDict = json.loads(result)
    rottenCol.insert_one(rottenDict)
rawImdbDF, rawRottenDF = LoadCsv()
cleanImdbDF, cleanRottenDF = CleanData(rawImdbDF, rawRottenDF)
mongoDB = MongoDBInit()
imdbCol, rottenCol = MongoCollection(mongoDB)
MongoInsert(imdbCol, rottenCol, cleanImdbDF, cleanRottenDF)