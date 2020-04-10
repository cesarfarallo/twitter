from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import twitter_credentials as tw
from google.cloud import pubsub_v1
#from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


#analyzer = SentimentIntensityAnalyzer()

project_id = "cf-learninggcp-123"
topic_name = "tweets"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

#consumer key, consumer secret, access token, access secret.
ckey=tw.CONSUMER_KEY
csecret=tw.CONSUMER_SECRET
atoken=tw.ACCESS_TOKEN
asecret=tw.ACCESS_TOKEN_SECRET

class listener(StreamListener):

    def on_data(self, data):
        try:
            data = json.loads(data)
            tweet = data['text']
            created_at = data['created_at']
            source = data['source']
            usuario = data['user']['name']
            ubicacion = data['user']['location']
            coordenadas = data['geo']
            time_ms = data['timestamp_ms']
 #           vs = analyzer.polarity_scores(tweet)
  #          sentiment = vs['compound']
            mensaje = json.dumps({"twitter": tweet, "time_stamp": time_ms, "created_at": created_at, "source": source, 'usuario': usuario, 'ubicacion': ubicacion, 'coordenadas': coordenadas})

            print(mensaje)
            future = publisher.publish(topic_path, mensaje)
            print(future.result())

        except KeyError as e:
            print(str(e))
        return(True)

    def on_error(self, status):
        print(status)

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["fibertel", "cablevision", "@personal"])