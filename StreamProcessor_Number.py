import datetime
import os
import sys
#os.environ['SPARK_HOME']='/var/lib/xPatterns-5.0/spark-1.3.1/'
os.environ['SPARK_HOME']='/var/lib/xPatterns/spark/'
#sys.path.append('/var/lib/xPatterns-5.0/spark-1.3.1/python/lib/py4j-0.8.2.1-src.zip')
sys.path.append('/var/lib/xPatterns/spark/python/lib/py4j-0.8.2.1-src.zip')


from StreamProcessor import xStreamProcessor
from pyspark.sql import *
from pyspark.sql.types import *


def process_line(txt):
        txts = txt.split('|')
        if txts is None or len(txts)<4:
            #return (txts,'','',{})
            return ['','','none','-1','{}']
        ip = txts[0]
        port = txts[1]
        dt = txts[2]
        user = txts[3]
        amnt = float(txts[4])
        big_trans = 0


        if abs(amnt)>=10000:
            big_trans = 1
        ddt = None
        try:
            ddt = datetime.datetime.strptime(dt,"%Y%m%d%H%M%S")
        except Exception as e:
            print e.message

        is_in_odd_day = 0
        is_at_odd_time = 0

        if ddt is not None:
            if ddt.weekday()==6 or ddt.weekday()==7 :
                is_in_odd_day = 1
            if ddt.hour<7 or ddt.hour>22:
                is_at_odd_time = 1



        return [ip,port,dt,user,amnt,big_trans,is_in_odd_day,is_at_odd_time]



def process_rdd(rdd):

    hiveContext = HiveContext(rdd.context)
    try:
        fields = [StructField('ip', StringType(), True),
                  StructField('port', StringType(), True),
                  StructField('date_time', StringType(), True),
                  StructField('user', StringType(), True),
                  StructField('amount', DoubleType(), True),
                  StructField('big_trans', IntegerType(), True),
                  StructField('is_in_odd_day', IntegerType(), True),
                  StructField('is_at_odd_time', IntegerType(), True)
                  ]
        schema = StructType(fields)
        # Apply the schema to the RDD.
        schemaRDD = hiveContext.applySchema(rdd, schema)
        #schemaRDD.printSchema()
        schemaRDD.insertInto(tableName='trans_stream',overwrite=False)
        #schemaRDD.saveAsParquetFile('parq.parquet')
    except Exception as e:
        print e

class StreamProcessor_Number(xStreamProcessor):

    def process_stream(self):
        words = self.dstream.map(lambda line: process_line(line))
        #words.pprint()
        if words.count()>0:
            #words.pprint()
            words.foreachRDD(process_rdd)

if __name__ =='__main__':

    sm1 = StreamProcessor_Number(ip=None,port=1235)
