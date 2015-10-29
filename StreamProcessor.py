__author__ = 'claudiub'
import socket
import os
import sys
#os.environ['SPARK_HOME']='/var/lib/xPatterns-5.0/spark-1.3.1/'
sys.path.append('/var/lib/xPatterns/spark/python')
os.environ['SPARK_HOME']='/var/lib/xPatterns/spark/'
#sys.path.append('/var/lib/xPatterns-5.0/spark-1.3.1/python/lib/py4j-0.8.2.1-src.zip')
sys.path.append('/var/lib/xPatterns/spark/python/lib/py4j-0.8.2.1-src.zip')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import HiveContext

class xStreamProcessor:
    ip = socket.gethostbyname(socket.gethostname())
    port = 9999
    dstream = None
    sc = None
    ssc = None

    #def __init__(self,ip=None,port=None,spark_master = 'spark://localhost:7077'):
    def __init__(self,ip=None,port=None,spark_master = 'mesos://10.0.2.85:5050'):
        if ip is not None:
            self.ip = ip
        if port is not None:
            self.port = port
        self.sc = SparkContext(master=spark_master,appName='StreamProcessor')
        self.ssc = StreamingContext(self.sc, 1)
        #self.ssc.checkpoint(directory=None)
        hiveContext = HiveContext(self.sc)
        hiveContext.sql('DROP TABLE IF EXISTS default.tweet_stream')
        hiveContext.sql('CREATE TABLE IF NOT EXISTS default.tweet_stream (ip STRING, port STRING, date_time STRING, user STRING, msg STRING)')

        hiveContext.sql('DROP TABLE IF EXISTS default.email_stream')
        hiveContext.sql('CREATE TABLE IF NOT EXISTS default.email_stream (ip STRING, port STRING, date_time STRING, \
        fr STRING,to STRING, subject STRING, content STRING, subject_sentiment INT, content_sentiment INT, \
        subject_power INT, content_power INT,  subject_topic INT, content_topic INT, fraud_score DOUBLE)')

        hiveContext.sql('DROP TABLE IF EXISTS default.email_graph')
        hiveContext.sql('CREATE TABLE IF NOT EXISTS default.email_graph (fr STRING,to STRING, dt STRING)')

        hiveContext.sql('DROP TABLE IF EXISTS default.trans_stream')
        hiveContext.sql('CREATE TABLE IF NOT EXISTS default.trans_stream (ip STRING,port STRING, date_time STRING, user STRING, amount DOUBLE, \
        big_trans INT, is_in_odd_day INT, is_at_odd_time INT)')

        self.dstream = self.ssc.socketTextStream(self.ip, self.port)


        self.process_stream()

        self.ssc.start()
        self.ssc.awaitTermination()

    def process_stream(self):
        parts = self.dstream.flatMap(lambda line: line.split("|"))
        words = parts.map(lambda p: p[3])
        pairs = words.map(lambda word: (word, 1))
        wordCounts = pairs.reduceByKey(lambda x, y: x + y)

        # Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.pprint()
if __name__ =='__main__':
    sm1 = xStreamProcessor(ip=None,port=1235)
