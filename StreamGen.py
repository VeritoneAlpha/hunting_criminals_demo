__author__ = 'claudiub'
import socket
import datetime


class StreamGen:
    bad_words = ['write-off',
         'foul',
         'unforgiving',
         'buddy',
         'concerned',
         'disgusting',
         'dissatisfied',
         'breach',
         'advice',
         'advisors',
         'risk',
         'confidential',
         'misunderstanding',
         'liability',
         'manipulated',
         'worst',
         'tickets',
         'greenbacks',
         'positions',
         'settle',
         'unsatisfied',
         'regret',
         'penalties',
         'prediction',
         'jump',
         'disciplined',
         'investment',
         'sue',
         'cell',
         'undeliverable',
         'endorse',
         'bird',
         'unprofessional',
         'met',
         'change',
         'wait',
         'coerced',
         'frustrating',
         'changed',
         'contradiction',
         'ignoring',
         'accomplished',
         'suitable',
         'changes',
         'wig',
         'love',
         'owed',
         'win',
         'ipo',
         'private',
         'regulatory',
         'bribery',
         'crisis',
         'market',
         'sympathetic',
         'illegal',
         'insider',
         'recommend',
         'memoranda',
         'door',
         'neglected',
         'aware',
         'dismal',
         'ashamed',
         'objections',
         'hurry',
         'cheapest',
         'embezzled',
         'scapegoat',
         'culpability',
         'jackpot',
         'predict',
         'share',
         'fuck',
         'stole',
         'counterfeit',
         'information',
         'thing',
         'complaining',
         'arrangement',
         'delay',
         'hot',
         'endorsement',
         'absolute',
         'stock',
         'fu',
         'rude',
         'incompetence',
         'disgusted',
         'wrong',
         'unacceptable',
         'shortcomings',
         'misappropriation',
         'lost',
         'mislead',
         'inform',
         'shore',
         'responsibility',
         'deceive',
         'carelessness',
         'exclusive',
         'forfeit',
         'mission',
         'embezzlement',
         'outraged',
         'embezzle',
         'patronize',
         'fined',
         'nonpublic',
         'compelling',
         'misunderstandings',
         'loophole',
         'brainer',
         'launder',
         'negligent',
         'complain',
         'prohibit',
         'books',
         'insurance',
         'issue',
         'correct',
         'molly',
         'worse',
         'dire',
         'terrible',
         'lousy',
         'appropriateness',
         'churning',
         'unsupported',
         'blank',
         'implicated',
         'miss',
         'arbitration',
         'forgery',
         'nobody',
         'manipulation',
         'believe',
         'discretionary',
         'anxiety',
         'objectionable',
         'grey',
         'bet',
         'questionable',
         'false',
         'correction',
         'suspicious',
         'low-priced',
         'arrangment',
         'comfortable',
         'conspiracy',
         'sell',
         'lie',
         'afraid',
         'unsuitable',
         'incentive',
         'recommended',
         'concerns',
         'performance',
         'competence',
         'play',
         'sure',
         'object',
         'placement',
         'consequences',
         'angst',
         'cover',
         'unreasonable',
         'clean',
         'annoyed',
         'misrepresented',
         'gold',
         'show',
         'cheat',
         'memorandum',
         'cheap',
         'merit',
         'portfolio',
         'find',
         'distressing',
         'nervous',
         'explain',
         'unhappy',
         'crime',
         'failed',
         'dispute',
         'unbeatable',
         'cheaper',
         'report',
         'hedge',
         'unethical',
         'secondary',
         'course',
         'falsify',
         'bad',
         'fair',
         'unsatisfactory',
         'sec',
         'culpable',
         'misappropriated',
         'failsafe',
         'promised',
         'checks',
         'bought',
         'missing',
         'sold',
         'blame',
         'wtf',
         'wth',
         'news',
         'cow',
         'fault',
         'pdt',
         'contract',
         'foreign',
         'disappointment',
         'cancel',
         'table',
         'trust',
         'disciplinary',
         'expensive',
         'informed',
         'commission',
         'secret',
         'po',
         'collectible',
         'understand',
         'doubts',
         'patronizing',
         'conspirator',
         'fearful',
         'cash',
         'value',
         'error',
         'situation',
         'falsified',
         'mistake',
         'launderer',
         'embezzling',
         'reports',
         'rid',
         'examine',
         'pay',
         'advise',
         'document',
         'disagree',
         'bulletproof',
         'terribly',
         'opportunity',
         'off',
         'falsification',
         'auditor',
         'appalled',
         'grossly',
         'excessive',
         'negligence',
         'money',
         'shocked',
         'incurring',
         'discipline',
         'guaranteed',
         'tax',
         'guarantees',
         'complaint',
         'laundering',
         'fye',
         'fyi',
         'implicate',
         'disappointed',
         'loss',
         'gift',
         'dishonest',
         'loser',
         'benefit',
         'lose',
         'security',
         'deal',
         'coerce',
         'contacts',
         'avoid',
         'conservative',
         'critical',
         'discretion',
         'manipulate',
         'apathetic',
         'prohibited',
         'losing',
         'riskless',
         'conceal',
         'misleading',
         'act',
         'concession',
         'lied',
         'rectify',
         'owe',
         'abusive',
         'ppm',
         'cancelable',
         'appropriate',
         'promise',
         'apologize',
         'impressed',
         'verbal',
         'shhhh',
         'frustration',
         'troubling',
         'kiting',
         'offering',
         'offer',
         'failure',
         'goals',
         'surrender',
         'rush',
         'fraud',
         'inside',
         'back-date',
         'windfall',
         'convince',
         'problem',
         'upset',
         'ship',
         'annoying',
         'check',
         'shit',
         'tix',
         'no',
         'tip',
         'guarantee',
         'forge',
         'poor',
         'poop',
         'liquidate',
         'bribe',
         'friends',
         'mischaracterized',
         'annuity',
         'fraudulent',
         'oral',
         ]
    good_words = ['music','movie','song','actor','pictures','america']

    backlog = 5
    ip =  socket.gethostbyname(socket.gethostname())
    port = 9999
    socket = None
    client = None
    address = None
    thread = None
    users = []

    sim_date_time = datetime.datetime.strptime('20130101000000','%Y%m%d%H%M%S')
    interval = 1000 #milliseconds

    def __init__(self,ip = '127.0.0.1',port='9999'):
        self.users=[]
        self.load_users()
        if len(ip)>0 and self.ip is None:
            self.ip=ip
        if len(port)>0:
            self.port = int(port)
        print 'opening stream on ip',self.ip,' and port',self.port
        if self.socket is not None:
            self.close()
        else:
            self.socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            self.socket.bind((self.ip,self.port))
            self.socket.listen(self.backlog)
            print "socket connected...we're listening..."
            try:
                print 'Starting listening in a new thread...'
                self.hookup()
                #self.thread = threading.Thread(target = self.hookcup)
                #self.thread.start()
            except Exception as e:
                print e

    def hookup(self):
        import random
        print 'Thread started'
        self.client, self.address = self.socket.accept()
        print 'Connected...'
        while 1:
            try:
                self.write(str(random.randint(0,100)))
            except Exception as e:
                print e.message
                self.client, self.address = self.socket.accept()

    def write(self,message):
        # message = message.replace('\n',' ').replace('\r',' ')
        if self.client is None:
            pass
        else:
            self.sim_date_time += datetime.timedelta(milliseconds=self.interval)
            s= self.ip+'|'+str(self.port)+'|'+self.sim_date_time.strftime("%Y%m%d%H%M%S")+'|'+message+'\n'
            print s
            try:
                self.client.send(s)
            except Exception as e:
                print e.message
                self.client, self.address = self.socket.accept()

    def close(self):
        if self.socket is not None:
            self.socket.close()


    def load_users(self):
        import cPickle
        f = open('enron_people.pickle','r')
        enron_people = cPickle.load(f)
        f.close()
        self.users=[]
        for person in enron_people:
            if '@' in person:
                self.users.append(person)

if __name__=='__main__':
    stream1 = StreamGen(port='1234')
    stream1.close()