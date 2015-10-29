import random
import datetime
import time
from StreamGen import StreamGen

class StreamGen_Number (StreamGen):

    def get_name(self,txt):
        result = set()
        if '@' in txt:
            if ' ' in txt:
                spl = txt.split(' ')
                for sp in spl:
                    if '<' in sp or '>' in sp:
                        continue
                    result.add(sp)
            else:
                result.add(txt)
        return result

    def hookup(self):
        print 'Thread started'
        self.client, self.address = self.socket.accept()
        print 'Connected...'
        i=0
        while 1:
            try:

                self.sim_date_time += datetime.timedelta(milliseconds=30000)
                if self.sim_date_time.year>2015:
                    break
                i+=1
                rnd_hour = random.randint(1,60)

                if self.sim_date_time.hour % rnd_hour != 0:
                    continue

                rnd_user_id = random.randint(0,len(self.users)-1)
                rnd_number = random.uniform(19.5,500)
                if i % 10 ==0:
                    sign = random.uniform(2.0,3.0)
                else:
                    sign = random.uniform(-1.0,-2.0)

                rnd_number *= sign

                if rnd_user_id % 7 == 0:
                    rnd_number = random.uniform(15.1,1500.3)
                if rnd_user_id % 5 == 0:
                    rnd_number = random.uniform(150.2,15000.5)
                if rnd_user_id % 4 == 0:
                    rnd_number = random.uniform(20.1,50000.1)
                if rnd_user_id % 3 == 0:
                    rnd_number = random.uniform(20.01,500.2)
                if rnd_user_id % 11 == 0:
                    rnd_number = random.uniform(15.1,10000.7)
                if rnd_user_id % 13 == 0:
                    rnd_number = random.uniform(20.01,150000.7)

                rnd_hour = random.randint(1,60)
                if self.sim_date_time.hour % rnd_hour == 0:
                    self.write(self.users[rnd_user_id]+'|'+str(round(rnd_number,2)))
                    time.sleep(0.6)
            except Exception as e:
                print e.message
                self.client, self.address = self.socket.accept()

if __name__=='__main__':
    stream1 = StreamGen_Number(port='1235')
    stream1.interval=30000
    stream1.close()