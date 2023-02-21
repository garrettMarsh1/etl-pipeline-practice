
 
import pandas as pd
from pymongo import MongoClient
import psycopg2 as pg
import time

mongoDBConnection = 'mongodb+srv://garytwotimes:LRcstOD6ZUblNJ26@cluster0.i7c5ze6.mongodb.net/test'


class SendToDB():


    def SendToMongo(df, collectionName, mongoDBConnection):
        df = pd.read_excel('dags/excelData/reviews.xlsx')
        
        
        #connecting to mongo client
        client = MongoClient(mongoDBConnection)
        #creating a database titled airflowTestingClusterDB
        db = client.airflowTestingClusterDB
        
        
        collection = db[collectionName]
        #create schema for the collection
        schema = {
            "product_name": "string",
            "review_title": "string",
            "comment": "string",
            "rating": "string",
            "date": "string",
            "username": "string",
            "verified_purchase": "string"
        }
        #insert the data from the xlsx file into the collection according to the schema

        #create schema for each row of the collection accord to the meta data of the dataframe
        db.amazonReviews.create_index([('product_name', 1), ('review_title', 1), ('comment', 1), ('rating', 1), ('date', 1), ('username', 1), ('profile_url', 1), ('verified_purchase', 1)])
        #insert the data from the dataframe into the collection drop duplicates
        #insert data according to the indexes
        collection.insert_many(df.to_dict('records'), ordered=False)
        #find duplicates according to schema and drop them


        
        
        #close the client
        client.close()
        print('Data sent to MongoDB')
    

    
   




    # def SendToPostgres( df='df', user='postgres', password="0000", host='localhost', port='5433'):
    #     conn = pg.connect(user='postgres', password="0000", host='localhost', port='5433')
    #     df = pd.read_excel('dags/excelData/reviews.xlsx')
    #     df['comment'] = df['comment'].astype(str)
    #     engine = create_engine('postgresql://postgres:0000@localhost:5433/airflowTestingDB')
    #     #if any of the data being entered is a duplicate move to next review
    #     df = df.drop_duplicates(subset=['product_name','review_title', 'comment', 'rating', 'date', 'username', 'profile_url', 'verified_purchase'], keep='first')
    #     df = df.reset_index(drop=True)
    #     df = df.drop(columns=['product_name'])
    #     df = df.rename(columns={'product_name': 'product_name', 'review_title': 'review_title', 'comment': 'comment', 'rating': 'rating',
    #     'date ': 'date', 'username': 'username', 'profile_url': 'profile_url', 'verified_purchase': 'verified_purchase'})
    #     df.to_sql('amazonReviews', engine, if_exists='append', index=False)
    #     print('postgres data inserted')



    
    # print('postgres data inserted')










