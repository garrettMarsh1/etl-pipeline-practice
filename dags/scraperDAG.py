from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
#from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
import pymongo as MongoClient

# defining a function to scrape reviews from Amazon
def scrapeReviews():
    import time
    import pandas as pd
    from dags.scraper.AmazonScraper import AmazonScraper
    # initializing empty list of reviews
    reviews = []
    # creating an instance of the AmazonScraper class
    amz_scraper = AmazonScraper()
    # setting the URL of the product page we want to scrape
    product_url = 'https://www.amazon.com/LG-77-Inch-Refresh-AI-Powered-OLED77C2PUA/product-reviews/B09RMSPSK1/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
    # loop over the first three pages of reviews and scrape them
    for page_num in range(3):
        reviews.extend(amz_scraper.scrapeReviews(url=product_url, page_num=page_num))
        # wait for 1 second before scraping next page
        time.sleep(1)
    print('Reviews scraped')
    # create a pandas dataframe from the scraped reviews
    df = pd.DataFrame(reviews)
    print('Dataframe created')
    # save the dataframe to an excel file
    df.to_excel('dags/excelData/reviews.xlsx', index=False)
    print('Dataframe saved to excel')

# defining a function to send scraped data to MongoDB
def sendToDB():
    #import pymongo as MongoClient
    import pandas as pd
    from dags.scraper.SendToDB import SendToDB
    # sending the dataframe to MongoDB atlas
    SendToDB.SendToMongo(df ='df', collectionName="amazonReviews", mongoDBConnection='mongodb+srv://garytwotimes:LRcstOD6ZUblNJ26@cluster0.i7c5ze6.mongodb.net/test')
    #SendToDB.checkForDuplicates(df='df')
    print('Data sent to mongo atlas')

# defining default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
}

# setting up the DAG with the required tasks
with DAG(dag_id='scraperDAG', default_args=default_args, start_date=datetime(2020, 1, 1),
    schedule='@hourly', description="Scraping e-commerce reviews",
     tags=["web scraping", "Garrett"], catchup=False) as dag:
    
    # python operator to trigger the scrapeReviews function
    scrapeReviews = PythonOperator(
        task_id='scrapeReviews',
        python_callable=scrapeReviews,
        dag=dag
    )
    
    # python operator to trigger the sendToDB function
    sendToDB = PythonOperator(
        task_id='sendToDB',
        python_callable=sendToDB,
        dag=dag
    )

# setting the dependencies between the tasks
scrapeReviews >> sendToDB