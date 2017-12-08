

import storm
import datetime
import time
import boto3
import re
import json

ACCESS_KEY="AKIAJ4PR7T5JL5PU73VQ"
SECRET_KEY="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"
REGION="us-east-2"

# today = date.today()

#Connect to DynamoDB
# conn_db = boto3.dynamodb.connect_to_region(REGION,
#    aws_access_key_id=ACCESS_KEY,
#    aws_secret_access_key=SECRET_KEY)


dynamodb = boto3.resource('dynamodb', aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY,
                            region_name=REGION)
   
table = dynamodb.Table('tweetsentiment')



TERMS={}

#-------- Load Sentiments Dict ----
sent_file = open('AFINN-111.txt')
sent_lines = sent_file.readlines()
for line in sent_lines:
    s = line.split("\t")
    TERMS[s[0]] = s[1]

sent_file.close()

def analyzeData(tweet):
    #Add your code for data analysis here

    sentiment=0.0

    tweet = json.loads(tweet)
    if tweet.has_key('text'):
        text = tweet['text']        
        text=re.sub('[!@#$)(*<>=+/:;&^%#|\{},.?~`]', '', text)
        splitTweet=text.split()

        for word in splitTweet:
            if TERMS.has_key(word):
                sentiment = sentiment+ float(TERMS[word])

    return sentiment
    # return True

class MyBolt(storm.BasicBolt):
    def process(self, tup):
        data = tup.values[0]

        output = analyzeData(data)
        
        # result= "Result: "+ str(output)

        result= str(output)

        #Store analyzed results in DynamoDB
        # table = conn_db.get_table("tweetsentiment")

        # item_data = {
        #     'data':str(data),
        #     'prediction':str(output),
        # }

        # item = table.new_item(
        # hash_key=str(today),
        # range_key=str(data),
        # attrs=item_data
        # )
        # item.put()

        # ts=time.time()
        # timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        # text = json.loads(result['data'])['text']

        # data = str(data)

        # # text = data[]

        # text = data['text']

        data = json.loads(data)

        table.put_item(
            Item={
                "timestamp": data['timestamp_ms'],
                "data": data['text'], #data,
                "prediction": result
                 }
        )
                            
        storm.emit([result])


MyBolt().run()



