import gspread 
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd 

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

creds = ServiceAccountCredentials.from_json_keyfile_name('erm_tracker.json', scope)

client = gspread.authorize(creds)

tracker = client.open_by_key('myKey')
hms = tracker.get_worksheet(0)
ba_comm_cis = tracker.get_worksheet(1)
cj = tracker.get_worksheet(2)
ed = tracker.get_worksheet(3)
cul_hos = tracker.get_worksheet(4)

hms = (pd.DataFrame(hms.get_all_records())
	     .rename(columns = {' ' : 'Company Name'}))

ba_comm_cis = pd.DataFrame(ba_comm_cis.get_all_records())
cj = pd.DataFrame(cj.get_all_records())
ed = pd.DataFrame(ed.get_all_records())
cul_hos = pd.DataFrame(cul_hos.get_all_records())

appended = (pd.concat([hms,ba_comm_cis,cj,ed,cul_hos], sort =True)
		      .drop(columns = ['Company Name','Location(s)','Partnership Status (New Lead, Prospect, Partner)','Has employer hired NLU stu/grad for internship or job?','Link to Employer Careers Page'], axis=1)
		      .rename(columns = {'Relevant Major(s)' : 'tags','Link to Employer Handshake Page' : 'Link'}))
appended = appended.filter(['tags','Link'])				

no_hs = appended[~appended.Link.str.startswith('h')].index 

appended.drop(no_hs, inplace=True)

appended[['Link','identifier']] = appended.Link.str.rsplit("/", n=1 ,expand = True)
appended = appended.drop(columns = ['Link'])
appended['tags'] = appended['tags'].str.strip()
to_replace = {'CUL/B&P':'CUL,B&P', 'HOS/BUS':'HOS,BUS','HOS/CUL':'HOS,CUL','HOS/CUL/B&P':'HOS,CUL,B&P','HOS/CUL/BUS':'HOS,CUL,BUS','Communications; Human Services':'Communications, Human Services'}
appended['tags'].replace(to_replace = to_replace, inplace = True)
				
appended[['t1','t2','t3','t4','t5','t6','t7','t8','t9','t10','t11']] = appended.tags.str.split(',', expand = True)
appended = (appended.drop(columns = ['tags'])
					.melt(id_vars = ['identifier'], var_name = 'm_tags', value_name = 'name'))
appended['name'] = appended['name'].str.strip()
hs_labels = {'ABA':'ba aba', 
			 'ABS':'ba abs',
			 'B&P':'bapa',
			 'BUS':'ba ba',
			 'Business':'ba ba',
			 'CJ':'ba cj',
			 'Communications':'ba ac',
			 'Counseling':'ms coun',
			 'Criminal Justice':'ba cj',
			 'CSIS':'bs cis',
			 'CUL':'ba cula',
			 'Culinary':'ba cula',
			 'ECE':'ba ece',
			 'Ed':'education',
			 'Education':'education',
			 'Health Care Leadership':'bs hcl',
			 'HOS':'ba hos',
			 'Hospitality':'ba hos',
			 'Human Service' :'ba hms',
			 'Human Services':'ba hms',
			 'Human Serivces':'ba hms',
			 'Management':'bs mgt',
			 'Psychology':'ba psych',
			 'Special Ed':'ba spe',
			 'HR':'ms hrmd',
			 }
appended['name'].replace(to_replace = hs_labels, inplace = True)
appended['name'] = appended['name'].str.strip()
blanks = []

for idx,x in appended['name'].iteritems():
	if x == None:
		blanks.append(idx)
	elif x == '': 
		blanks.append(idx)
appended.drop(blanks, inplace = True)
appended['identifiable_type'] = 'Employer'
appended['user_type'] = None
appended.drop(columns = ['m_tags'])
appended = appended.filter(['identifier','identifiable_type','user_type','name'])
appended.to_csv('master.csv')
