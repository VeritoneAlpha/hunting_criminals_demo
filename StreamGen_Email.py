import random
import cPickle
import datetime
from StreamGen import StreamGen
import time

class StreamGen_Email (StreamGen):
    email_file = 'enron_df_small.pickle'
    emails = None

    def load_emails(self):
        f = open(self.email_file,'r')
        self.emails = cPickle.load(f)
        f.close()

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
        print 'Loading Emails... takes some time :)'
        if self.emails is None:
            self.load_emails()

        print 'Thread started'
        print 'Emails: ',len(self.emails)
        self.client, self.address = self.socket.accept()
        print 'Connected...'
        i=0
        while 1:
            time.sleep(0.45)
            try:
                self.sim_date_time += datetime.timedelta(milliseconds=random.randint(10000,60000))
                if self.sim_date_time.year>2015:
                    break
                if self.sim_date_time.weekday()==7 or self.sim_date_time.weekday()==6:
                    continue
                if i<len(self.emails)-1:
                    i+=1
                else:
                    i=0
                row = self.emails.iloc[i]

                fr = '~'.join(list(self.get_name(row['mailfrom'])))
                to = '~'.join(list(self.get_name(row['torecipients'])))
                subj = row['subject']
                msg = row['content']

                self.write(fr+"|"+to+"|"+subj.replace('|',' ')+"|"+msg.replace('|',' '))


            except Exception as e:
                print e.message
                self.client, self.address = self.socket.accept()

if __name__=='__main__':
    stream1 = StreamGen_Email(port='1237')
    stream1.interval=30000
    stream1.close()