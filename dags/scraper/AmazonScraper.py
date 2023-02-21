# Import necessary libraries
import re  # regular expression library for pattern matching
from collections import namedtuple  # for named tuple data structures
import time  # for pausing in between requests
import requests  # for HTTP requests
from bs4 import BeautifulSoup  # for HTML parsing
import pandas as pd  # for data manipulation
import psycopg2 as pg2  # for PostgreSQL database connectivity

# NamedTuple definition for UserReview
UserReview = namedtuple('UserReview', ['product_name', 'review_title', 'comment', 'rating', 'date', 'username', 'profile_url', 'verified_purchase'])

# Class definition for AmazonScraper
class AmazonScraper:
    # Compile patterns for matching review dates and product names from URLs
    review_date_pattern = re.compile('(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?) \d+, \d{4}')
    product_name_pattern = re.compile('^https:\/{2}www.amazon.com\/(.+)\/product-reviews')

    # Constructor function
    def __init__(self):
        # Create a browser session with headers
        self.session = requests.Session()
        self.session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv: 87.0) Gecko/20100101 Firefox/87.0'
        self.session.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        self.session.headers['Accept-Language'] = 'en-US,en;q=0.5'
        self.session.headers['Connection'] = 'keep-alive'
        self.session.headers['Upgrade-Insecure-Requests'] = '1'

    # Function to scrape reviews
    def scrapeReviews(self, url, page_num, filter_by='recent'):
        """
        Scrape Amazon product reviews from given URL.

        Args:
            url (str): Amazon product review URL.
            page_num (int): Page number of reviews to scrape.
            filter_by (str, optional): Filter reviews by 'recent' or 'helpful'. Defaults to 'recent'.

        Returns:
            list: List of UserReview namedtuples containing scraped review information.
        """
        try:
            # Set the URL to the appropriate review page
            review_url = re.search('^.+(?=\/)', url).group()
            review_url = review_url + '?reviewerType=all_reviews&sortBy={0}&pageNumber={1}'.format(filter_by, page_num)

            # Print progress message
            print('Processing {0}...'.format(review_url))

            # Make the HTTP request
            response = self.session.get(review_url)

            # Get the product name from the URL
            product_name = self.product_name_pattern.search(url).group(1) if self.product_name_pattern.search(url) else ''
            if not product_name:
                print('url is invalid. Please check the url.')
                return
            else:
                product_name = product_name.replace('-', ' ')

            # Parse the HTML response with BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            review_list = soup.find('div', {'id': 'cm_cr-review_list' })    

            # Create an empty list for storing reviews
            reviews = []

            # Find all reviews on the page
            reviews = []  # Initialize an empty list to store reviews

            product_reviews = review_list.find_all('div', {'data-hook': 'review'})  # Find all divs with 'review' as data-hook attribute

            for product_review in product_reviews:  # Iterate through all found product reviews

                # Extract review details from each product review
                # Review title
                review_title = product_review.find('a', {'data-hook': 'review-title'}).text.strip()  
                # Verified Purchase status
                verified_purchase = True if product_review.find('span', {'data-hook': 'avp-badge'}) else False  

                # Review text
                review_body = product_review.find('span', {'data-hook': 'review-body'}).text.strip()  
                
                # Review rating
                rating = product_review.find('i', {'data-hook': 'review-star-rating'}).text  

                # Review date
                review_date = self.review_date_pattern.search(product_review.find('span', {'data-hook': 'review-date'}).text).group(0)  

                # Reviewer username
                username = product_review.a.span.text  
                
                # Reviewer profile URL
                user_profile = 'https://amazon.com/{0}'.format(product_review.a['href'])  # Reviewer profile URL

                # Create a named tuple representing the User Review and append it to the list of reviews
                reviews.append(UserReview(product_name, review_title, review_body, rating, review_date, username, user_profile, verified_purchase))
            
            # Return the list of User Reviews
            return reviews  

        except Exception as e:
            print(e)
            return 0 
    

  


























    


