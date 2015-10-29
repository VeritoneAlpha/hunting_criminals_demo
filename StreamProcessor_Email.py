import json
import os
import sys
#os.environ['SPARK_HOME']='/var/lib/xPatterns-5.0/spark-1.3.1/'
os.environ['SPARK_HOME']='/var/lib/xPatterns/spark/'
#sys.path.append('/var/lib/xPatterns-5.0/spark-1.3.1/python/lib/py4j-0.8.2.1-src.zip')
sys.path.append('/var/lib/xPatterns/spark/python/lib/py4j-0.8.2.1-src.zip')

from StreamProcessor import xStreamProcessor
from pyspark.sql import *
from pyspark.sql.types import *

import nltk
stp_wds=set(nltk.corpus.stopwords.words('english'))


def tokenize_txt(tmp):
    result_freq={}
    from nltk.tokenize import sent_tokenize
    for sent in sent_tokenize(tmp):
        words=nltk.word_tokenize(sent)
        for w in words:
            if w not in stp_wds and len(w)>1:
                if len(w)>30 or '/' in w or '\\' in w or '--' in w or '~' in w or '#' in w or '_' in w:
                    continue
                if w in result_freq:
                    result_freq[w.lower()]+=1
                else:
                    result_freq[w.lower()]=1
    return result_freq


def tokenize_email(txt):
        txts = txt.split('|')
        if txts is None or len(txts)<6:
            return ['','','none','-1','{}']
        ip = txts[0]
        port = txts[1]
        dt = txts[2]
        fr = txts[3]
        tos = txts[4]# .split('~')
        subj = txts[5].encode('utf-8')
        content = txts[6].encode('utf-8')

        subj_freq={}
        content_freq = {}
        try:
            subj_freq = tokenize_txt(subj)
            content_freq = tokenize_txt(content)
        except Exception as e:
            subj_freq['ERROR']=str(type(e))+'  '+e.message

        subj_sentiment = -1
        content_sentiment = -1
        subj_power = -1
        content_power = -1
        subj_topic = -1
        content_topic = -1
        fs = 0.0

        if 'ERROR' not in subj_freq:
            subj_sentiment= get_sentiment(subj_freq)
            content_sentiment = get_sentiment(content_freq)
            subj_power= get_power_submissive(subj_freq)
            content_power = get_power_submissive(content_freq)
            subj_topic= get_topic(subj_freq)
            content_topic = get_topic(content_freq)
            fs= fraud_score(content_freq)
        return [ip,port,dt,fr,tos,json.dumps(subj_freq),json.dumps(content_freq),subj_sentiment,content_sentiment,subj_power,content_power,subj_topic,content_topic,fs]


def get_relations(txt):
    txts = txt.split('|')
    if txts is None or len(txts)<6:
        return ['none','none','0']
    dt = txts[2]
    fr = txts[3]
    if '~' in txts[4]:
        tos = txts[4].split('~')
    else:
        tos = [txts[4]]
    result = []
    for to in tos:
        if '@' in to and len(to)>5:
            result.append([fr,to,dt])
    return result

def append_emails(rdd):
    hiveContext = HiveContext(rdd.context)
    try:
        fields = [StructField('ip', StringType(), True),
                  StructField('port', StringType(), True),
                  StructField('date_time', StringType(), True),
                  StructField('fr', StringType(), True),
                  StructField('to', StringType(), True),
                  StructField('subject', StringType(), True),
                  StructField('content', StringType(), True),
                  StructField('subject_sentiment', IntegerType(), True),
                  StructField('content_sentiment', IntegerType(), True),
                  StructField('subject_power', IntegerType(), True),
                  StructField('content_power', IntegerType(), True),
                  StructField('subject_topic', IntegerType(), True),
                  StructField('content_topic', IntegerType(), True),
                  StructField('fraud_score', DoubleType(), True)
                  ]

        schema = StructType(fields)
        # Apply the schema to the RDD.
        schemaRDD = hiveContext.applySchema(rdd, schema)
        #schemaRDD.printSchema()
        schemaRDD.insertInto(tableName='email_stream',overwrite=False)
        #schemaRDD.saveAsParquetFile('parq.parquet')
    except Exception as e:
        print e

def append_graph(rdd):
    hiveContext = HiveContext(rdd.context)
    try:
        fields = [StructField('fr', StringType(), True),
                  StructField('to', StringType(), True),
                  StructField('dt', StringType(), True),
                  ]

        schema = StructType(fields)
        # Apply the schema to the RDD.
        schemaRDD = hiveContext.applySchema(rdd, schema)
        #schemaRDD.printSchema()
        schemaRDD.insertInto(tableName='email_graph',overwrite=False)
        #schemaRDD.saveAsParquetFile('parq.parquet')
    except Exception as e:
        print e

def get_topic(tokens):
    if tokens is None:
        return 100
    topics={0:['click','free','offer','online','price','receive','link','web','list','save'],
        1:['emission','environmental','air','permit','plant','facility','unit','epa','water','station'],
        2:['page','court','employees','law','labor','worker','union','federal','employer','act'],
        3:['travel','hotel','roundtrip','fares','special','offer','city','visit','miles','deal'],
        4:['game','yard','defense','allowed','fantasy','point','passing','rank','against','team'],
        5:['american','bush','world','attack','country','government','president','war','campaign','member'],
        6:['project','request','approval','resource','application','construction','manager','transaction','site','date'],
        7:['texas','longhorn','team','game','play','brown','season','top','orange','football'],
        8:['user','data','access','password','center','server','problem','address','sap','outage'],
        9:['deal','trading','risk','position','transaction','book','power','product','credit','gas','bid','price','prices','market'],
        10:['company','stock','financial','dynegy','investor','shares','billion','credit','rating','analyst',
            'firm','fund','partner','ventures','technology','round','capital','services']
        }
    topic_names={0:'email ads',1:'environment',2:'labor law',3:'travel',4:'fantasy football',5:'politics',6:'projects',7:'sports',8:'it',
             9:'trading',10:'company'}
    total = 0.0
    for token in tokens:
            total+=tokens[token]
    if total==0.0:
        return -1
    result={}
    max_score=-1
    max_topic=100
    for topic in topics:
        score=0.0
        for word in topics[topic]:
            if word in tokens:
                score+=tokens[word]
        score=score/total
        result[topic]=score
        if score>max_score:
            max_score=score
            max_topic=topic
    return max_topic


def fraud_score(tokens):
    if tokens is None:
        return 0.0
    wds=set(['write-off',
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
         ])
    total = 0.0
    bad_words = 0.0
    for token in tokens:
        total+=tokens[token]
        if token in wds:
            bad_words+=tokens[token]

    if total==0.0:
        return 0.0
    return bad_words/total

def get_sentiment(tokens):
    if tokens is None:
        return -1
    topics={1:['pardon','magnetic','desirable','sleek','integrity','conjure','originality','outwit','tenacity','regal',
'pride','worth','wondrous','collaborate','rescue','co-operation','compassion','dynamic','gratification','courageous',
'prize','solution','viable','flashy','enhance','steadfastness','straight','triumph','enjoy','veritable',
'consistent','welfare','elegant','fabulous','valiant','even','desirous','liberty','reverently','endorse',
'non-violence','niche','hero','festive','refinement','protection','humility','celebration','elaborate','culminate',
'credit','cogent','mentor','permit','baptize','suitable','intrigue','astute','golden','fantastic',
'secure','mobility','amiability','patience','moral','impartiality','mediate','call','calm','survive',
'holy','successful','ascribe','award','aware','warm','adult','excellent','adamant','abundance',
'join','sweeten','accomplishment','advocacy','cuddle','substantiate','endow','decorative','radiance','ingenuity',
'assent','accurateness','attract','guarantee','provide','verify','travel','amazing','significance','actuality',
'truthful','swiftness','dignified','beauty','delicacy','loyalty','plausibility','modest','mesh','law',
'meaningful','splendid','effective','appreciate','arbitrate','greet','upside','goodbye','order','modesty',
'devote','consent','fortify','innovative','responsive','fidelity','fit','comprehend','better','valor',
'admirable','overcome','pleasurable','glorify','vouchsafe','enrich','amenable','commemorate','lifelong','brightness',
'absolve','spotless','affiliate','reasonable','fitness','luck','consolidate','durability','marital','ameliorate',
'dawn','toleration','unbound','content','fellowship','encourage','sane','heavenly','independence','associate',
'free','bestow','amazement','warmhearted','jubilant','inexpensive','tolerant','flawless','attune','aspire',
'symbolize','non-violent','organize','angelic','reassure','fantasy','sober','radiate','comic','clarity',
'revival','plentiful','compromise','unbroken','reparation','asset','embellish','wisdom','mercy','congratulatory',
'exultation','nominate','matter','mirth','acclaim','willingness','quench','resound','mind','vastness',
'brainy','mint','genuine','responsible','communicative','elegance','luxury','effectiveness','propitious','conducive',
'visionary','simplify','affirmation','plenty','morality','constancy','potency','treaty','calmness','principle',
'confederation','marvel','sanctuary','inspire','endear','rapture','alight','sage','brilliance','subtle',
'availability','acquit','divinity','quaint','undoubtedly','cupid','bliss','rich','mend','adequate',
'continuity','leisure','foremost','considerate','relish','godliness','monumental','adherence','enchant','fragrant',
'fullness','fair','sensitivity','gladness','allure','best','accede','afloat','cooperation','approach',
'adventuresome','accord','devout','upfront','dedication','absorbent','protect','sublime','logic','distinction',
'contribution','self-respect','confide','inquisitive','hygiene','comprehension','majestic','speedily','comical','gratitude',
'beneficiary','confident','charisma','accentuate','communion','interest','basic','flexible','lovely','adroitly',
'pro','idol','beloved','prosperous','chum','excite','uncommon','harmonize','coherent','dote',
'gracious','save','affirm','neat','aid','congratulation','preeminent','jest','amour','uplift',
'make','utilitarian','harmony','world-famous','exertion','independent','commonsense','hand','delight','renaissance',
'descry','opportunity','kid','stupendous','humble','game','inherit','bonny','savings','contact',
'abound','etiquette','appease','just','athletic','dignify','repose','human','extol','enlighten',
'visualization','agile','ease','advancement','exalt','obedient','board','innocent','righteous','humanity',
'usable','survival','auspicious','discreet','indicative','affability','sanitary','unique','befit','dignity',
'gift','gifted','excitedness','splendor','invaluable','security','superlative','beneficial','right','envision',
'deal','flatter','easy','revel','dear','intellect','nobleman','congratulate','humor','creative',
'witty','adorable','palatable','confer','constructive','stylish','adhesion','super','trophy','empowerment',
'trustworthy','blithe','ensure','cherish','motivate','civil','honeymoon','intellectual','accommodate','respectable',
'familiarize','indispensable','suffice','illustrious','victorious','greatness','happy','overjoyed','offer','fascination',
'heal','vitality','understandable','prettily','safeguard','true','luxuriant','counsel','invincible','bargain',
'generosity','adore','decorate','adorn','liberal','classic','soothe','subsist','revitalize','liberalism',
'faithful','authoritative','excel','interested','frolic','subsidize','welcome','polite','comely','magnificence',
'entrust','cohesion','loyal','serious','intimacy','undoubted','precedent','befriend','remarkable','dance',
'relieve','mild','grateful','remarkably','abundant','resourceful','discern','idealism','swoon','aristocratic',
'compensate','persevere','protective','advantage','navigable','exact','pious','impressive','economize','dig',
'noiseless','settle','excellence','round','brave','measurable','affiliation','adulation','stately','cleanliness',
'assistance','reunite','appeal','boost','alertness','satisfy','guardian','inauguration','attainment','entertain',
'lustrous','decipher','momentous','merriment','redemption','brilliant','patriotic','priceless','reclaim','adjustable',
'inference','logical','morale','marvelous','memorable','surmount','positiveness','vivid','positive','give',
'live','upheld','plaything','subsistence','acceptable','repentance','validity','virtue','behalf','edible',
'indescribable','privy','harmonious','achieve','laughter','heart','gratify','refine','council','admirer',
'chic','intelligible','purr','productive','savor','eloquent','pure','optimal','allowable','unforgettable',
'sworn','dominance','paramount','peerless','impartial','vivacious','flourish','renovation','temperate','infallibility',
'natural','amuse','purify','purification','fluent','propriety','dreamland','legitimate','cute','shield',
'playful','lyric','thoughtfulness','culmination','adhesive','avid','solace','attraction','factual','conquer',
'discuss','equality','stabilize','thankful','advent','realistic','amnesty','revere','authenticity','amusement',
'giddy','accomplish','concur','intercourse','profit','abide','boundless','tingle','imperative','rational',
'correct','assurance','worthiness','sympathize','palatial','oasis','lucrative','matchless','accommodation','care',
'patronage','advance','workmanship','gusto','honest','recompense','invitation','profess','assuredly','exotic',
'symmetry','merrily','open','necessarily','bloom','stimulation','fiery','convene','friend','festivity',
'hug','pomp','statuesque','cleanse','inventor','accordance','optimism','glossy','synthesis','proactive',
'usefulness','angel','sumptuous','correction','premier','sagacity','prosperity','fashionable','efficient','terrific',
'ideal','objective','amiable','normal','attractiveness','donation','adaptability','energize','proud','steady',
'satisfactorily','precise','accession','merit','bright','upgrade','favor','treasure','impunity','betrothal',
'stand','uppermost','enthusiasm','awareness','godlike','assistant','admiration','infallible','conjunction','celebrate',
'prime','pinnacle','temperance','salutation','wonder','voluntary','steadfast','useful','legitimacy','fearless',
'inseparable','adept','spare','comeback','talented','dedicate','cognizance','allowance','adroit','genial',
'satisfaction','gaiety','fruitful','laudable','sensible','companionship','capable','almighty','dependable','peaceful',
'educated','workable','minister','careful','sound','prominence','notoriety','cherub','crusader','zenith',
'sincere','embrace','graceful','return','fame','pay','preparatory','confidant','poetic','deference',
'noble','safe','enjoyment','assist','companion','delicate','cheery','tradition','purposeful','magical',
'staunchness','accountable','relief','alleviate','coordinate','reward','justify','valuable','lavish','reputable',
'polish','subscription','dauntless','scamper','improvement','rosy','joyful','real','exult','spectacular',
'earnest','preference','credentials','reap','bravery','sensational','subscribe','miracle','sanity','benefit',
'miraculous','passionate','ceremonial','satisfactory','laugh','deduce','respect','divine','capability','prolific',
'legal','gladden','moderate','zest','intimate','notable','poise','innocence','squarely','fondness',
'piety','favorite','practical','practicable','fertile','bountiful','renovate','harness','lively','pleasantry',
'gleam','glean','politeness','condone','immaculate','complete','tranquility','regard','sophisticated','rouse',
'handsome','romantic','receptive','flirt','conqueror','affection','brotherly','taste','fellow','imagination',
'reliability','tranquil','fill','completeness','deserve','sanguine','accolade','repent','valid','reassurance',
'championship','standardize','shelter','seriousness','important','brotherhood','equitable','prudent','unselfish','conclusive',
'applause','groom','nutrient','consider','thoughtful','bequeath','cooperative','fairness','meditation','religious',
'attentive','homage','foster','fervor','health','premium','straightforward','carefree','candid','congenial',
'manageable','enjoyable','heaven','benevolent','celebrity','ceaseless','adjust','trustworthiness','vanquish','dexterity',
'portable','colossal','self-contained','intricate','enhancement','pass','affable','casual','quicken','conscience',
'stood','advantageous','learner','eminent','nurse','everlasting','multitude','outgoing','empower','beacon',
'selective','relaxation','thrift','conviction','compliance','experience','acquittal','sweetness','whimsical','aptitude',
'establish','readily','attendance','eye','composure','distinct','revelation','cultivation','gorgeous','garnish',
'affluence','befitting','formality','company','particular','mellow','glad','learn','tenacious','prompt',
'share','accept','attain','sense','cultivate','dazzle','comfort','fruition','utilize','rapport',
'sacred','vigilance','ensemble','simplicity','anoint','interpose','indomitable','ethics','authentic','coax',
'meticulous','responsibility','pleasure','help','closeness','flaunt','staunch','prodigy','harmless','exuberance',
'ally','colleague','good','motivated','consign','adventurous','association','infer','bless','friendship',
'exquisite','pamper','resolute','beneficent','connect','reverent','viability','jolly','applaud','safety',
'paradise','fortitude','darling','outrun','salute','boldness','forgive','backer','pleasant','extraordinary',
'qualify','generate','heroic','acquaint','curtsey','apocalypse','major','fancy','obedience','sympathy',
'glitter','arbitration','peaceable','utilization','blossom','revive','hallowed','obtainable','wonderful','commensurate',
'saint','aristocracy','elaboration','appreciation','luckily','favorable','consult','prodigious','flattery','grace',
'kind','marriage','restful','motivation','outstanding','reunion','shrewd','affirmative','caress','cognizant',
'blissful','remodel','onset','populous','serene','wholesome','breadwinner','subsidy','charm','significant',
'prestige','achievement','sturdy','clear','humanitarian','traditional','charitable','clean','liberate','nobility',
'perseverance','gold','fond','defender','melody','fine','completion','justice','assertion','worthy',
'versatility','pretty','equity','prowess','permission','strut','famous','remedy','courage','tribute',
'instinctive','inaugurate','resolve','progressive','perfume','common','precaution','loveliness','art','intelligence',
'potent','culture','venerate','defense','manly','gaily','expert','distinctive','positivity','worth-while',
'please','commemoration','woo','myriad','donate','compliment','candor','complementary','masterful','untouched',
'succeed','premise','distinguish','lyrical','sensitive','protector','approve','majesty','sweet','allegiance',
'community','adaptation','rave','civilize','bolster','create','acceptance','creativity','sociable','affectionate',
'sweetheart','champion','gay','precision','adaptable','moderation','understand','reliable','civility','tolerate',
'smile','admire','healthy','fun','guide','backing','moralistic','astound','decoration','ecstasy',
'funny','communal','benign','grand','forgiveness','clearness','commune','bonus','comprehensive','indispensability',
'negotiate','appreciable','decency','aggregate','robust','salvation','persuasive','cheer','inspirational','consistency',
'savvy','romanticize','scrupulous','altruistic','thorough','vigilant','love','profitable','impetus','relevancy',
'resourcefulness','eager','studious','dare','relevance','reinstate','excited','dependability','enable','unlimited',
'aggregation','redeem','clout','helpful','outright','pertinent','hardy','consensus','popular','softness',
'essential','humorous','magnificent','back','understood','mighty','sprightly','amicable','irresistible','acquaintance',
'proficient','novelty','balmy','providence','civilization','superiority','sympathetic','patient','benefactor','agreement',
'respite','togetherness','faith','modernity','graduation','forgave','shrewdness','shiny','credible','repair',
'reverence','conquest','appropriate','primarily','conscientious','contributor','adeptness','renewal','consultation','suit',
'forward','glow','invite','healthful','glamour','authority','upright','patriot','gallantry','foresight',
'maturity','verification','mature','highlight','autonomous','individuality','popularity','reconcile','mindful','precept',
'cure','rally','surmise','endurance','peace','consummate','nice','venerable','energetic','smitten',
'oblige','lucid','backbone','luxurious','restoration','heroine','rightful','summit','fresh','enrichment',
'cheerful','renown','aristocrat','adjustment','uphold','suave','stable','enlightenment','friendly','virtuous',
'affix','prudence','affinity','recreation','respectful','marry','compassionate','tenderness','mobilize','pledge',
'playmate','exuberant','ardent','append','reconciliation','proffer','justification','cozy','communicate','jointly',
'nimble','meet','consideration','great','engage','talent','survivor','resurrect','benevolence','commendable',
'honor','chaste','deservedly','hospitable','apt','extravagance','proprietary','staple','confidence','contentment',
'agreeable','clever','beauteous','impress','cooperate','heartily','train','charity','augment','exhilaration',
'praise','fulfill','nurture','rehabilitation','purity','indulgence','coincident','patron','earnestness','rapt',
'fulfillment','sought','hit','surge','educational','daring','picturesque','buoyant','fortunate','attachment',
'insistent','justifiably','comedy','intelligent','luster','attend','tact','prance','unhurried','light',
'allow','insight','chivalry','perfect','superior','industrious','chosen','glee','heroism','willing',
'prosper','adornment','impervious','richness','kiss','kindness','handy','reestablish','thrill','profound',
'mastery','radiant','feast','perfectionism','delightful','commodious','perfectionist','truth','champ','flair',
'illuminate','special','gallant','entertainment','defend','mate','thrifty','stud','hilarious','honorable',
'adaptive','frank','promptly','clarify','tempt','allies','courteous','feasible','keen','allied',
'facilitate','stimulate','ethical','betroth','signify','eminence','filial','quality','opportune','salutary',
'glisten','privacy','festival','durable','realistically','agility','innovate','accompaniment','blameless','haven',
'glamorous','treatise','aspiration','riches','frugal','compensation','lover','distinguished','devotion','ingenious',
'imaginative','captivation','chivalrous','partnership','comfortable','gentle','buy','sincerity','competency','inform',
'skillful','able','admittance','adjunct','willful','triumphal','competence','accuracy','collaboration','resplendent',
'courtesy','fervent','enthusiastic','outlive','familiarity','longevity','topmost','pleased','principal','knowledge',
'hopeful','amplify','hope','prosecute','righteousness','adherent','commendation','familiar','lucky','accrue',
'perfection','wise','stability','tolerance','affluent','thrive','triumphant','luminous','invulnerable','genius',
'beautify','progress','complement','ability','importance','joy','efficiency','firmness','merry','approval',
'precious','optional','thank','empathy','diligent','joke','assure','admit','glorious','relevant',
'coexistence','commend','competent','maximize','coddle','tremendous','decent','steadiness','treat','upbeat',
'improve','sufficient','meritorious','acclamation','arisen','main','sanctify','plain','ripen','value',
'entrepreneurial','advisable','amenity','likable','ripe','apprehend','encouragement','lust','optimistic','partner',
'conserve','cohort','productivity','romance','unity','painstaking','merciful','soundness','privileged','grandeur',
'covenant','ball','recline','intercede','acknowledgement','well','scruples','credibility','restore','accurate',
'obtain','surpass','skill','coordination','happiness','generous','console','absorption','onward','arrest',
'attractive','smart','resolved','definitive','ample','punctual','know','amply','prominent','like',
'success','audible','therapeutic','accessible','alive','immortal','motive','proper','home','supportive',
'trust','lead','farsighted','appreciative','discretion','forgiven','comestible','obey','poignant','offset',
'posterity','sparkle','refuge','actual','freedom','idolize','compatible','courtly','augmentation','concession',
'upward','outset','alliance','faithfulness','commitment','congregate','promise','esteem','lawful','doubtless',
'jubilee','rejoice','commission','support','glimmer','nourish','elate','enchantment','gain','capitalize',
'versatile','tactics','glory','reinforcement','warmth','excitement','convince','baptism','cordial','supreme',
'eagerness','contribute','crusade','goodness','education','mutual','efficacy','unimpeachable','boom','uttermost',
'semblance','ecstatic','nourishment','arbiter','inspiration','appoint','commencement'],
        0:['dissolution','obstruction','protest','thirst','controversial','hate','aggression','unfeeling','vile','scold',
'unsteadiness','agitator','daze','sputter','immature','forbidden','lure','slothful','dispense','void',
'lurk','inhibit','distort','smack','disobedient','disturb','pinch','loneliness','brutish','deplorable',
'force','monotonous','tired','horn','depraved','crave','infiltration','rigorous','kidnap','decadence',
'even','incompatible','ruthless','hazy','insinuate','mischievous','sunder','blur','succumb','avert',
'endanger','wastefulness','shriek','violate','doldrums','unwillingness','study','ridicule','foolishness','controversy',
'harass','superstitious','lonesome','anarchist','menial','cancellation','envious','divide','stray','loveless',
'sneer','shoddy','stern','devastation','insecurity','negative','cannibal','insult','plod','strike',
'barbarous','wary','expose','warp','hurt','grief','disruption','qualm','hole','tariff',
'sorry','overrun','shoot','err','somber','worn','whack','guise','root','quarrel',
'defile','pollute','wane','deadweight','unjust','heartless','impair','degenerate','indifferent','cursory',
'farce','agony','nuts','damage','hot','disappoint','resignation','gamble','undesirable','badly',
'fever','mess','insecure','lag','vicious','misunderstanding','unguarded','lay','ambivalent','austere',
'alienate','revolt','complexity','order','unsteady','restriction','sickly','ghastly','strangle','wrought',
'wickedness','wilt','hypocrisy','unforeseen','fix','haphazard','secede','tramp','precipitate','unfortunate',
'press','unemployed','drowsiness','collide','break','renunciation','diseased','detachment','repulsive','shameful',
'indulge','interrupt','cynical','gloom','sleepless','mistrust','explode','superficiality','frantically','capricious',
'misbehave','mean','interruption','truant','whip','contend','depress','misfortune','fraught','laid',
'remorse','sluggish','outburst','barrier','veto','lawless','fugitive','untruth','struggle','abate',
'fret','shaggy','refugee','rumple','oppose','hopeless','regress','naughty','dunce','bombard',
'deplete','clamorous','excommunication','service','sarcasm','indeterminate','fumble','inconvenient','bitter','illiterate',
'paltry','collapse','fanatical','esoteric','inexact','crawl','peculiar','rebuff','anxiety','outlaw',
'bitterness','matter','guilt','painful','iron','devious','rattle','pessimism','mind','mine',
'untrained','antagonistic','rat','lull','horde','relapse','alibi','uncouth','writhe','timidity',
'object','vexing','prohibitive','addict','drought','panic','grave','illogical','treasonous','antitrust',
'alarm','partition','worsen','ridiculous','oppress','fussy','discontent','bomb','hunger','retire',
'precarious','undue','uneasiness','poison','queer','bail','capitulate','spite','undignified','wasteful',
'cripple','inhibition','disgust','enslave','hoard','hostility','denounce','baffle','roundabout','slanderous',
'nasty','volatility','overbearing','bar','subservience','broken-hearted','collusion','idiotic','bad','steal',
'erroneous','unworthy','disaster','unexpectedly','dispensability','miserable','temptation','artificial','startle','irk',
'disprove','scorn','lazy','confusion','ignorance','wear','censorship','contamination','undependable','drowsy',
'tyranny','accident','irregular','brat','fault','ill','against','brag','expense','eradicate',
'spear','irresponsible','epidemic','incessant','lowly','petty','jobless','bewilder','futility','subside',
'detrimental','incompetent','fleeting','beastly','suppress','dismiss','bereavement','confinement','dissention','catch',
'sad','unpopular','exception','sly','apprehension','ugly','ail','slam','abrupt','thrash',
'subjugate','mangle','mistake','pessimistic','stink','sting','shame','dizzy','exile','sever',
'growl','severity','make','wound','rebellion','grapple','complex','split','contaminate','indictment',
'raid','fuss','unbearable','misery','hang','evil','hand','nix','haggard','rebut',
'intrusion','taint','impediment','incapable','bereave','stringent','sentence','anti-social','bruise','unfair',
'beset','disguise','plight','brandish','boot','hag','snarl','board','prison','falter',
'pitiful','vanity','tolerable','coarseness','submissive','skeptical','involuntary','disagreement','roughness','shadow',
'retreat','disadvantage','desire','brutality','uncivil','contradict','grizzly','hunt','dishonest','unnecessary',
'arbitrary','hung','notorious','starvation','battlefield','squander','deal','repudiate','deaf','dead',
'accost','hassle','displeasure','bore','clash','unavoidable','foe','convict','plot','brawl',
'burn','blackmail','confrontation','defensive','substitution','bury','chafe','limitation','restlessness','conceal',
'anomalous','choke','despair','commit','twitch','meddle','croak','sob','dilemma','anxiousness',
'infraction','bound','formidable','annoyance','deception','disinterest','fight','derision','fought','war',
'questionable','starve','immorality','hypocrite','heinous','failure','blindness','humiliate','absent','misinform',
'captive','prowl','pest','propaganda','disgrace','scandalous','fury','covert','unnerve','drive',
'mediocre','dent','injurious','discriminate','disputable','dying','floor','stagnant','unwilling','erosion',
'naive','handicap','liquidate','persecution','discharge','congested','bullet','quandary','withhold','rough',
'cheater','push','backward','subvert','revoke','particular','battle','rot','uneven','deceitful',
'flounder','sufferer','vex','charge','unpleasant','dissatisfied','terror','oversight','ride','eliminate',
'division','jerk','unfamiliar','sloppy','liability','turbulent','gloomy','mourn','trouble','blast',
'feeble','cool','dim','din','ruthlessness','tear','fragile','gun','encroach','revolution',
'regret','insignificant','injunction','obsolete','agitate','cost','bombardment','scarcity','perplexity','helpless',
'molest','acrimonious','curse','jeopardy','havoc','suspect','lazily','drunkard','explosion','abscond',
'unobserved','superstition','disclaim','detest','congestion','deceit','combatant','wait','box','boast',
'disavow','burdensome','ineffective','weird','ineffectiveness','bolt','insolent','madman','bloody','myself>',
'fake','crisis','senile','angry','ravage','wicked','club','refrain','downhearted','drown',
'dismal','degrade','demolish','collision','pretend','fatalistic','instable','impasse','sour','unruly',
'thief','crazy','downfall','critic','provoke','stole','craze','redundancy','bankrupt','stricken',
'nonchalant','inundated','meager','plaintiff','idleness','swore','unbelievable','mar','irrational','fascist',
'scoff','brazen','mad','waste','destructive','filthy','egotistical','disagreeable','stress','aimless',
'nosey','needy','thorny','jail','deceive','imprecision','offend','bungle','drunken','vagrant',
'forfeit','yearn','shake','horrify','cold','haughty','stupidity','suffocate','suspicion','antipathy',
'suspension','inefficiency','lonely','smash','killer','bashful','sarcastic','heedless','execute','feverish',
'drop','revert','careless','instability','vexation','subjection','wearisome','ailment','crushing','abnormal',
'aloof','counteract','hysteria','impurity','irony','interference','astray','trauma','ruinous','dominate',
'busybody','monster','siege','unfriendly','rebel','hobble','ominous','impose','wrong','turn',
'impure','quibble','bothersome','outcry','neglect','puny','blind','feign','suspend','indecent',
'clumsy','temporarily','languish','impetuous','thoughtless','wrath','murky','bite','callous','autocrat',
'shiver','tragic','persecute','bloodshed','gaudy','uninformed','orphan','hurtful','snatch','alas',
'vain','illegality','rival','stammer','extinguish','damned','decompose','troublesome','illness','capsize',
'sag','argument','slap','counteraction','conspiracy','anger','sap','pry','bait','isolate',
'wretch','fracture','destroy','hedonistic','blunt','commonplace','pain','oppressive','betrayal','assault',
'clatter','touchy','blockhead','hungry','scowl','jittery','intoxicate','shot','show','infuriate',
'cheap','trespass','sketchy','hack','aggressive','hustler','help','unkind','stormy','crime',
'fickle','outrage','stain','shrill','lapse','dispute','exaggeration','get','straggler','cannon',
'simplistic','unimportant','hedge','treachery','beseech','obliterate','indeterminable','overworked','stupid','liable',
'worrier','uneconomical','famine','unfit','recalcitrant','hell','culpable','stigma','concern','crooked',
'alarming','altercation','untrustworthy','forsake','pitiless','blame','undermine','oust','temper','belie',
'disappointment','delirium','utterance','evict','duty','madness','provocation','cancer','foreboding','colony',
'inexplicable','resentment','accusation','exploit','cancel','bizarre','runaway','intrude','underworld','consumptive',
'sinful','exhaustion','exasperate','homely','stalemate','vagueness','abject','crumble','bout','robber',
'predicament','trick','scum','unsatisfactory','dubious','blah','clutter','ironic','obnoxious','worry',
'resentful','harsh','insidious','breakdown','exhaust','mockery','defy','refusal','weed','devoid',
'diversion','unqualified','entanglement','upheaval','weep','bane','undependability','bondage','melodramatic','demean',
'inability','desolate','slime','violent','kill','scrape','perilous','blow','death','rigid',
'senseless','subversion','overthrow','struck','disrupt','disproportionate','frown','dark','inflation','darn',
'detract','secrecy','disadvantageous','snare','implicate','grim','distrustful','snore','terrorize','superfluous',
'tardy','wrongful','robbery','obstruct','falsehood','afflict','witchcraft','imprisonment','competition','erase',
'shortcoming','extinct','derisive','perverse','renounce','corrosive','exit','deficit','compulsion','knife',
'hustle','ration','slaughter','forgetfulness','uneasy','despicable','hectic','throw','tease','smear',
'jeer','paralysis','scornful','assassin','fallacy','prohibit','feudal','intervention','burglar','brute',
'stark','strict','unreliability','low','hysterical','mischief','fanatic','shroud','recoil','elimination',
'unreliable','belated','fraud','annihilate','rage','fabricate','extermination','sadness','dirty','sleazy',
'deadlock','powerless','ruffian','inflict','gratuitous','frighten','censor','ax','deterrent','casualty',
'incorrect','feud','devilish','idiot','novice','abolish','trample','unaccustomed','woeful','fruitless',
'desolation','damnable','unattractive','grudge','consternation','apathy','recklessness','fraudulent','mourner','prejudicial',
'banishment','liar','founder','beware','lack','weariness','unrest','wrestle','antagonize','reluctant',
'dreadful','wanton','catastrophe','scrutinize','hideous','tatter','worse','equivocal','infringement','discrepant',
'horror','fat','rogue','worst','fall','awful','maladjusted','deficient','ingratitude','fearful',
'desertion','undone','sank','manslaughter','withheld','preposterous','pass','affectation','misrepresent','destitute',
'hamper','darkness','poisonous','whimper','rascal','straggle','exasperation','crush','nullification','repugnant',
'turmoil','ferocious','revenge','disastrous','excess','tragedy','stupor','whine','pick','smuggle',
'vie','marginal','gunmen','coercion','injure','faint','implore','injury','flaw','erode',
'regrettable','stolen','domination','stick','broke','amputate','adulteration','doom','pretense','offender',
'ineffectual','abandon','stubborn','shark','atrocious','abandonment','conspire','coerce','exclude','scar',
'allege','condemn','awkward','desperation','acrimony','unlucky','reject','peril','gash','abdicate',
'rude','uprising','sneak','unsound','disable','numb','bleak','refuse','criticize','deluge',
'shady','spiteful','blunder','mislead','assailant','dread','retaliate','crafty','cockiness','torturous',
'displease','oblique','distortion','dawdle','daunting','suffer','arrogant','susceptible','famished','absence',
'clamor','bereft','fool','motley','sucker','hunter','immobility','vomit','frigid','belittle',
'misuse','unauthentic','complain','restrict','evade','inequality','subjugation','boisterous','intolerable','disorganized',
'harm','restless','short','hard','deride','fist','absurdity','anarchy','terrify','gullible',
'presumptuous','unnatural','childish','sickness','avenge','tiresome','shrivel','disobedience','dictatorial','pathetic',
'dirt','foible','difficulty','monotony','dire','clique','hapless','reproach','omit','terrible',
'insolence','audacity','prejudice','threat','expedient','dissatisfy','unhealthy','perturb','radical','belligerent',
'barbarian','sequester','wench','bland','miss','moan','differ','symptom','ostracize','patronize',
'divorce','doomsday','flimsy','storm','scheme','merciless','headache','imperfect','needle','boredom',
'stifle','rampant','ungrateful','lament','double','impersonal','outrageous','contrary','stale','risky',
'phobia','suspicious','defect','decadent','murder','servitude','engulf','gall','cumbersome','lie',
'evasion','cave','commotion','severe','tempest','useless','serve','disconcerted','yawn','unequal',
'seize','covet','unreasonable','strain','hindrance','barren','lying','fidget','despise','exterminate',
'agonize','aghast','clog','malignant','unarm','fine','aggravate','delinquent','paranoid','nervous',
'ruin','grotesque','unhappy','betray','fiend','circle','indifference','devastate','babble','resent',
'avarice','cheapen','procrastination','theft','distraction','enemy','contagious','obscure','cringe','coward',
'insensible','insane','cramp','dump','dumb','close','annihilation','horrid','unsettling','blurt',
'forlorn','wail','incompatibility','silly','avoidance','corruption','hardship','opposition','invisible','carelessness',
'quarrelsome','foreign','bandit','poverty','shortsighted','load','sloth','distress','point','dwindle',
'smother','hollow','improper','unsuccessful','indefinite','mumble','expensive','belt','decline','complication',
'devil','filth','raise','stamp','overlook','appall','miser','secret','extravagant','coarse',
'threaten','empty','fire','infect','cruelty','bitchy','fretful','rupture','loom','shove',
'conspirator','corrode','error','fun','pound','costly','agitation','guilty','demon','rip',
'confound','obstacle','frantic','stubbornly','rid','vanish','anguish','chase','dishonor','discredit',
'bristle','shirk','conflict','epithet','censure','impede','fabrication','infamous','lower','motionless',
'try','edge','toil','secession','grievance','chore','jeopardize','unseen','ludicrous','confiscation',
'competitive','ignoble','distracting','cocky','irritable','regardless','superficial','cut','vague','hater',
'complaint','emergency','condemnation','displace','cataclysm','standstill','bit','moody','knock','blemish',
'confront','strife','uproar','aggravation','unstable','foolish','disorder','transgress','paralyzed','goddamn',
'litter','scandal','selfishness','savage','scald','culprit','bogus','slash','prod','invade',
'apathetic','jumpy','run','wayward','lose','rue','ache','taboo','subtract','constraint',
'ordeal','frightful','lifeless','ambush','hatred','block','pollution','misbehavior','unnoticed','depose',
'deafness','nonsense','dissatisfaction','vagabond','liquidation','bankruptcy','lone','chide','overpower','inconsistency',
'mishandle','opponent','mundane','perplex','audacious','slump','scuffle','segregation','usurp','warlike',
'dull','skulk','rejection','inhumane','commiseration','slander','depreciate','aversion','repulse','curt',
'freak','oddity','unsafe','haunt','deprive','seethe','unwise','vehement','sullen','nag',
'objection','malicious','invalid','condescension','assassinate','confine','deviate','mock','scapegoat','besiege',
'prosecution','bravado','inflame','drag','suppression','drab','wrinkle','desert','hackney','fighter',
'vice','downcast','scream','impatient','bribe','inaccuracy','ignorant','menace','pervert','rubbish',
'commoner','alien','dispel','amiss','discordant','disposal','arrogance','obstinate','breach','dislike',
'fatal','nervousness','entangle','murderous','accuse','discouragement','shock','anxious','race','sedentary',
'enforce','chastise','encroachment','malice','mishap','unprofitable','crook','uncomfortable','abominable','odd',
'leakage','depression','rotten','volatile','capital','melancholy','punch','let','gloat','adulterate',
'conceit','screech','contradiction','haziness','stubbornness','avaricious','defeat','behead','danger','forbid',
'limp','confession','apprehensive','decrease','mortify','infest','manipulate','fed','illegal','germ',
'procrastinate','doubt','cram','defendant','sort','confiscate','shipwreck','credulous','infection','sore',
'insufficiency','ashamed','eccentricity','unfaithful','rivalry','annoy','disingenuous','meek','pout','challenge',
'recession','gruff','ineffectualness','violence','thirsty','coolness','inaccessible','tax','unhappiness','villain',
'hit','contempt','counterfeit','defective','awkwardness','tamper','negation','neurotic','delay','dependent',
'fearsome','weaken','deplore','sin','abrasive','incompetence','hazard','impulsive','nullify','discomfort',
'abuse','fatigue','ambiguous','lame','arduous','redundant','alienation','furious','addiction','irregularity',
'poor','disarm','rebellious','doubtful','spank','brittle','brood','exclusion','decay','guerrilla',
'dispose','blatant','wreck','criminal','demoralize','lunatic','muddle','negligent','front','flee',
'fled','crass','bewilderment','estranged','immoral','trap','shortage','disregard','scorch','thwart',
'tension','deficiency','static','irritation','banal','rumor','confess','unlawful','chaos','repress',
'venom','perish','spot','inferiority','undo','intruder','shun','embarrass','delinquency','shrew',
'scary','corrosion','hostile','spill','shred','uproot','sorrowful','scare','undid','confuse',
'affliction','deceptive','droop','vulgar','lamentable','immovable','attack','ferocity','misinformed','misunderstood',
'malady','shell','unscrupulous','punish','shallow','dreary','feint','too','absent-minded','manipulation',
'overwhelming','ultimatum','warfare','brusque','bother','aggressor','beg','calamity','torrent','misunderstand',
'false','beggar','neutralize','corrupt','torment','dictate','need','screw','bastard','afraid',
'costliness','opinionated','boastful','mix','divert','autocratic','spinster','disavowal','combat','harmful',
'skirmish','vengeance','rigor','demise','prisoner','nuisance','bloodthirsty','deny','upset','disease',
'crumple','absurd','fiasco','contradictory','constrain','inane','contemptuous','deviation','reprehensible','violation',
'wince','antiquated','traitor','slanderer','rebuke','passe','fear','trivial','envy','untimely',
'tire','craziness','jar','rash','reactive','ambiguity','wretchedness','inferior','topple','antagonism',
'beat','dope','massacre','cranky','prohibition','woe','darken','fallout','burglary','unjustified',
'antagonist','ghetto','discourage','dishearten','contemptible','dejected','defame','berserk','default','allegation',
'grab','omission','humiliation','expel','recede','admonition','unfavorable','desperate','incurable','dungeon',
'nightmare','monstrous','viper','horrible','crude','sabotage','derogatory','explosive','sorrow','grumble',
'interfere','embarrassment','complicate','spoil','absentee','tantrum','inundate','asunder','quitter','involve',
'careen','unclear','aggrieve','defiance','laugh','cynicism','coercive','accursed','unclean','quit',
'outsider','rusty','muddy','curtail','anomaly','butchery','tense','unmoved','overturn','shameless',
'banish','debtor','wily','intimidate','disdain','pandemonium','pretentious','depreciation','wild','venomous',
'chaotic','dissent','delusion','disapprove','adultery','aggressiveness','thud','regression','capture','denial',
'cross','defiant','adverse','outbreak','terrorism','frivolous','difficult','tedious','balk','reactionary',
'abyss','eccentric','outcast','fierce','destruction','retard','argue','trudge','nebulous','enrage',
'fail','excessive','negligence','discord','compel','mistaken','glum','disturbance','wee','pompous',
'drunk','diabolic','arrest','exempt','scoundrel','indignation','kick','untrue','harassment','complicity',
'taunt','burden','atrophy','condescending','debatable','loss','lost','loser','parasite','imprison',
'bafflement','sprain','adversity','decease','glare','shabby','scared','flagrant','puzzlement','stuffy',
'shrug','helplessness','competitor','disbelief','leak','avoid','hinder','noise','slight','worthless',
'adversary','slug','backwardness','sinister','slayer','swear','maladjustment','insufficient','diabolical','owe',
'loner','chronic','negate','pointless','repeal','impatience','buckle','disapproval','frustration','damn',
'treacherous','hazardous','mutter','shyness','admonish','plague','bum','assail','overflow','bug',
'selfish','detestable','yelp','soreness','dangerous','grieve','distract','cruel','limit','problem',
'piece','inadequate','meaningless','dearth','incredible','deadly','offensive','detain','sick','reckless',
'junk','traumatic','oppression','treason','frustrate','wallow','stab','animosity','deter','ramble',
'stunt','entreat','compete','abhor','unspeakable']
        }
    topic_names={0:'negative',1:'positive'}
    total = 0.0
    for token in tokens:
            total+=tokens[token]
    if total==0.0:
        return -1
    result={}
    max_score=-1
    max_topic=100
    for topic in topics:
        score=0.0
        for word in topics[topic]:
            if word in tokens:
                score+=tokens[word]
        score=score/total
        result[topic]=score
        if score>max_score:
            max_score=score
            max_topic=topic
    return max_topic


def get_power_submissive(tokens):
    if tokens is None:
        return -1
    topics={1:['founder','diplomacy','forbidden','relieve','dispel','manager','aristocrat','captain','undisputed','pardon',
'exterminate','arrogance','aristocratic','under','regal','lord','sovereignty','banish','adviser','queen',
'proctor','rescue','dispense','induce','appropriation','govern','exact','arrange','school','policeman',
'celebrity','leave','settle','delegate','encroach','inspection','enforce','triumph','vanquish','prevent',
'brave','force','encroachment','direct','preacher','recruitment','persuade','pass','ardent','hamper',
'victor','rope','educate','stately','colonel','eminent','liberty','endorse','unrestricted','body',
'hero','empower','herd','protection','let','standard','teacher','boast','pressure','conceit',
'justice','cogent','mentor','overseer','reclaim','authorize','elect','campaign','forbid','win',
'manage','clout','sanction','prompt','coercion','legislator','preeminent','spoke','certificate','army',
'subvert','stipulate','charge','assessor','confiscate','commissioner','parliament','condescension','muffle','award',
'excuse','domination','hold','overrun','entangle','impair','word','accomplishment','chastise','coalition',
'impel','appraisal','pick','control','patrol','patron','involve','coerce','council','exclude',
'handle','guarantee','sir','unwavering','court','ordinance','patronize','amazing','dominance','dignified',
'administrator','paramount','may','scoff','brazen','indomitable','lay','president','flourish','grow',
'egotistical','deluge','headquarters','assign','provide','chief','maintain','jail','allot','allow',
'preventive','predominate','order','cockiness','move','prohibition','statutory','embassy','fortify','committee',
'policemen','mayor','superior','police','exclusion','heroism','restrain','proclamation','might','overcome',
'glorify','richness','reassure','conquer','ambition','pompous','presidential','possess','term','stabilize',
'appropriate','capability','trap','found','amnesty','quarter','commonwealth','prohibit','suffrage','thwart',
'house','crushing','taught','sponsor','proclaim','try','accomplish','overcame','god','monitor',
'repress','laid','stud','dominate','legislative','shut','boldness','dictatorial','veto','free',
'managerial','bestow','advisor','amazement','teach','heroic','patronage','ministry','impose','gusto',
'keep','initiate','assuredly','permit','feed','organize','powerful','restrict','elder','instruct',
'eminence','drove','corporal','management','capitalize','autocrat','draft','sergeant','show','master',
'surmount','aristocracy','giver','enforcement','punish','esoteric','treasurer','reaffirm','overwhelming','elite',
'officiate','summon','king','rugged','require','instruction','diplomatic','recruit','merciless','distinguished',
'steer','palace','neutralize','strengthen','say','have','dictate','gall','take','regulate',
'strength','agency','boastful','majority','squad','chairman','command','autocratic','channel','auditor',
'urge','clash','deploy','prohibitive','resplendent','emancipation','prestige','bloodthirsty','achievement','statute',
'potency','sturdy','clear','cover','drive','constrain','liberate','oppress','nobility','pope',
'certification','please','jurisdiction','bring','rector','principal','impact','safeguard','sheriff','implementation',
'convinced','guidance','winner','employer','impunity','supremacy','rich','enslave','godlike','righteousness',
'dominant','permission','beat','stop','denounce','bear','strut','famous','spur','overbearing',
'bar','obliterate','godliness','priest','juror','landlord','relentless','withstand','requirement','overwhelm',
'riches','derogatory','stipulation','reinforce','triumphant','jury','arm','still','capacity','expert',
'invulnerable','posse','electoral','tend','state','chairmen','sequester','counsel','pitiless','accord',
'masterful','boss','spare','censorship','distinguish','outfit','haughty','pity','cop','goddess',
'disdain','influential','quiet','assure','admit','brag','senator','evict','eradicate','supply',
'smother','insist','condescending','exploit','coercive','trust','nomination','civilize','speak','strong',
'capable','union','certify','responsibility','confident','proprietor','dictator','entitle','privilege','tyranny',
'direction','educated','champion','oust','censor','stifle','dismiss','protect','deter','assert',
'controller','determination','minister','demand','instructor','shameless','gracious','politician','look','presidency',
'affirm','intimidate','reinstate','unwillingness','governor','employ','cast','suppose','guide','examiner',
'astound','commander','capture','grant','make','administration','surveillance','benign','world-famous','ruler',
'status','independent','junta','mockery','restraint','indispensability','effect','censure','hand','director',
'undaunted','nix','judgment','collar','destruction','floor','demean','congressmen','coordinate','prevention',
'ambassador','responsible','sentence','compel','executive','confiscation','press','select','cocky','surpass',
'abolish','prince','statesmen','discipline','liberation','dauntless','royal','subversion','arrest','board',
'stimulate','overthrow','righteous','administrative','designate','grip','government','chancellor','advocate','displace',
'royalty','judge','adjudication','bravery','prominent','proponent','triumphal','self-contained','benefit','superiority',
'officer','imprison','attorney','right','dean','senate','crown','back','authority','immortal',
'delegation','mighty','election','insistence','ballot','nobleman','oppressive','lead','armed','oppression',
'providence','manipulate','compulsion','leader','victory','survivor','run','obey','power','inspect',
'blackmail','lieutenant','step','leadership','keeper','trophy','empowerment','constraint','shield','ensure',
'emperor','alliance','suppression','depose','marshall','administer','guard','champ','determine','formidable',
'superintendent','almighty','support','legislation','sovereign','chide','overpower','strict','victorious','editor',
'way','greatness','statuesque','decisive','condone','head','heal','admonish','usurp','promote',
'hire','monopoly','invincible','official','extermination','preside','convince','limit','grasp','conqueror',
'autonomous','professor','repulse','supreme','subdue','constable','influence','haunt','general','deprive',
'examine','cure','curb','bicep','ax','ship','bishop','regime','contest','authoritative',
'boot','detain','afford','ambitious','test','unwilling','confine','mock','draw','championship',
'authoritarian','congress','shelter','desolation','backbone','bravado','establish','discharge','appoint','statesman',
'vice','assume','rule','banishment'],
        0:['appease','bearer','chain','devote','consent','knelt','commoner','maid','grateful','go',
'follow','fear','suffer','extol','capitulate','help','attentive','depend','tire','homage',
'pray','wishful','admission','sufferer','passive','exalt','fickle','stand','obedient','credulous',
'employee','petition','submissive','uncontested','exult','dependent','assistant','flattery','undergone','fan',
'discouragement','subjection','comply','look','suicide','beseech','dishearten','name','subservience','thankful',
'servant','withdrew','slavery','wait','succumb','admissible','adjust','subjugation','kneel','heed',
'revere','secretary','respond','attest','scare','victim','tired','flatter','scared','imprisonment',
'request','recruit','captive','venerate','yield','impossible','dispensability','patient','helpless','destitute',
'fidelity','unconditional','minority','reverent','helplessness','stood','oppressive','worsen','confess','lean',
'supporter','peasant','reverently','convict','tribute','appeal','worship','sacrifice','forsake','crouch',
'obey','abscond','apologetic','attention','inferiority','weary','respectful','please','murmur','ask',
'attendant','hapless','respect','salute','faith','enroll','laborer','redemption','receive','surrender',
'vulnerability','whine','dependence','bow','turn','admit','despair','recompense','receiver','commit',
'concession','undergo','menial','futility','duty','indebted','plea','conform','student','withdraw',
'confession','colony','private','obedience','put','follower','petitioner','submit','lowly','apologize',
'apply','trust','undergraduate','obligation','quit','confidence','service','harmless','buckle','due',
'concede','hallowed','implore','willing','compromise','under','underwent','reverence','rely','reparation',
'stomach','reaction','sag','regard','serve','excuse','weakly','recoil','beat','abject',
'believe','true','volunteer','unsuccessful','word','exultation','meek','fearful','coward','beg',
'believer','admire','waiter','suppose','rehabilitation','subjugate','conformity','powerless','devotion','imitation',
'defer','humble','willingness','inadequate','plead','mind','accept','crumble','servitude','bend',
'prayer','sap','tremble','vulnerable','weaken','gone','capture','afraid','participant','relapse',
'alibi','pay','belong','dishonor','gingerly','repent','reliance','application','learner','whip',
'unfortunate','sucker','disciple','porter','repentance','slave','abdicate','attend','oblige','oppression',
'loyalty','resign','react','questioner','modest','honor','prisoner','awe','withdrawn','relinquish',
'aimless','apology','bondage','applicant','appreciate','grind','entreat','loyal','pupil','incapable',
'subordinate','crumple']
        }
    topic_names={0:'submit',1:'power'}
    total = 0.0
    for token in tokens:
            total+=tokens[token]
    if total==0.0:
        return -1
    result={}
    max_score=-1
    max_topic=100
    for topic in topics:
        score=0.0
        for word in topics[topic]:
            if word in tokens:
                score+=tokens[word]
        score=score/total
        result[topic]=score
        if score>max_score:
            max_score=score
            max_topic=topic
    return max_topic


class StreamProcessor_Email(xStreamProcessor):
    def process_stream(self):
        emails = self.dstream.map(lambda line: tokenize_email(line))
        relations = self.dstream.flatMap(lambda line:get_relations(line))
        # emails.pprint()
        if emails.count()>0:
            emails.foreachRDD(append_emails)
        # relations.pprint()
        if relations.count()>0:
            relations.foreachRDD(append_graph)

if __name__ =='__main__':
    sm1 = StreamProcessor_Email(ip=None,port=1237)