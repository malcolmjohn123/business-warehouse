import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import db_credentials 
import psycopg2 as db
from psycopg2 import Error
from statistics import mean


def is_positive(review):
    scores_list = [sia.polarity_scores(sentence)["compound"] for sentence in nltk.sent_tokenize(review)]
    score = mean(scores_list)
    if score > 0  and score < 0.2:
        return "neutral"
    elif score>0:
        return "positive"
    else:
        return "negative"

sia = SentimentIntensityAnalyzer()
#retrieving data
with db.connect(**db_credentials.server_params) as conn:
    with conn.cursor() as curs:
        query = 'select * from sentiment_data_set'
        curs.execute(query)
        records = curs.fetchall()
        for row in records:
            review_category = is_positive(row[8])
            query = "update sentiment_data_set set review_category=" + "'"+ review_category+"' where reviewid='" + row[7] + "';";
            curs.execute(query)
            conn.commit()
        






