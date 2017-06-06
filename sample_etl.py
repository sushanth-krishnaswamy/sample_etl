#! usr/bin/python


import requests
import json
import unicodecsv
import email
import smtplib
import mimetypes
import email.mime.application
import codecs
import mysql.connector
from datetime import datetime, timedelta


# This function retreievs the access token and passes it onto the the needed fucntions, thereby not visibly displaying the access key.
def access_token_retrieve():

	payload = {

    'grant_type' : 'client_credentials',
    'client_id' : 'insert client id here',
    'client_secret' : 'insert the client secret here',

	}

	url = "insert URL for retrieveing the authentication token from the API. Example shown below."
	#url = "https://backstage.taboola.com/backstage/oauth/token"   --- Taboola API's example.
	r = requests.post(url, data = payload)

	data = json.loads(r.content)
	return data['access_token']

# This function retrieves all the first stage of information from the API for filtering. (example: accounts or other higher level info through which we need to loop through)
def accounts_list_retrieve():

	url_main = "insert the URL here"
	#Example for retrieving accounts for taboola API - url1 = "https://backstage.taboola.com/backstage/api/advertisers/"

	headers1 = {"Authorization": "Bearer "+access_token}

	r = requests.get(url1, headers=headers1)

	json_output1 = r.content

	json_data1 = json.loads(json_output1)

	advertisers_list = json_data1["results"]

	accounts_list=[]

	for i in advertisers_list:
		accounts_list.append(i["account_id"])

	return accounts_list

# This function generates the report in the form of a JSON object and loads the necessary information into the database.
def report_retrieve_db_load():


	host = "https://backstage.taboola.com/backstage/api/1.0/"
	extra = "/reports/campaign-summary/dimensions"
	headers = {"Authorization": "Bearer "+access_token}

	dimensions = "campaign_site_day_breakdown"

	#start_date="2017-02-21T00:00Z"
	#end_date="2017-02-21T23:59Z"
	#report_date = "2017-02-21"



	#This segment generates yesterday's date in UTC format to send it to the API
	report_date_obj = datetime.utcnow()-timedelta(1)
	report_date = str(report_date_obj.date())
	start_date = report_date+"T00:00Z"
	end_date = report_date+"T23:59Z"

	#This file contains the credentials for the database.
	testFile = open("./db_config.json")
	data = json.load(testFile)



	mydb = mysql.connector.connect(host=data["prod"]["host"],
					    user=data["prod"]["username"],
					    password=data["prod"]["password"],
					    db=data["prod"]["db"],
					    port=int(data["prod"]["port"]))
	testFile.close()
			
	cursor = mydb.cursor()

	query_check = "select 1 from campaign_site_day_breakdown where date = "+"'"+report_date+"'"
	cursor.execute(query_check)
	datum = cursor.fetchall()

	if datum:
		query_truncate = "delete from campaign_site_day_breakdown where date = "+"'"+report_date+"'"
		cursor.execute(query_truncate)
		mydb.commit()

	else:
		pass
			
	for items in acc_list:
			
		site = host+items+extra
		url = site+"/"+dimensions+"?"+"start_date="+start_date+"&"+"end_date="+end_date

		r = requests.get(url,headers=headers)

		try:

			json_output = r.content
			json_data = json.loads(json_output)
			list_insert = json_data.get("results")
		except ValueError:
			continue

		else:
		
		#cursor.execute('set global max_allowed_packet=67108864')
		#INSERT the data in the variable list_insert to the database.
			cursor.executemany("""INSERT INTO campaign_site_day_breakdown(date,site,site_name,campaign,clicks,impressions,spent,ctr,cpm,cpc,cpa,cpa_actions_num,cpa_conversion_rate,blocking_level,currency)VALUES(%(date)s,%(site)s,%(site_name)s,%(campaign)s,%(clicks)s,%(impressions)s,%(spent)s,%(ctr)s,%(cpm)s,%(cpc)s,%(cpa)s,%(cpa_actions_num)s,%(cpa_conversion_rate)s,%(blocking_level)s,%(currency)s)""",list_insert)

	mydb.commit()

	#this selects the columns required for the report.	
	query = "SELECT DISTINCT cs.date as date, cs.campaign as campaign,cd.campaign_name as campaign_name, cs.site_name as site_name,cs.site as site,cs.clicks as clicks,cs.impressions as impressions,cs.spent as spent,cs.ctr as ctr,cs.cpm as cpm, cp.cpc as cpc,cs.cpa as cpa,cs.cpa_actions_num as cpa_actions_num,cs.cpa_conversion_rate as cpa_conversion_rate,cs.blocking_level as blocking_level,cs.currency as currency FROM campaign_site_day_breakdown cs INNER JOIN campaign_day_breakdown cd ON cs.campaign = cd.campaign AND cs.date = cd.date INNER JOIN campaign_cpc cp ON cs.campaign = cp.campaign WHERE cs.date = "+"'"+report_date+"' AND cs.clicks > 0"
	#print query
	cursor.execute(query)
	sql_data = cursor.fetchall()

	#email part

	recipients = ['recepient1@domain.com','recepient2@domain.com']
	
	msg = email.mime.Multipart.MIMEMultipart()
	msg['Subject'] = "Sample report for - "+report_date
	msg['From'] = "sender@domain.com"
	msg['To'] = ",".join(recipients)

	

	body = email.mime.Text.MIMEText(" Attached is the report from the *company's* API . Thank you :)")
	msg.attach(body)


	field_names = [i[0] for i in cursor.description]
	filename = "results-"+report_date+".csv"

	with codecs.open("./reports/"+filename,'w+') as csvfile:
		writer = unicodecsv.writer(csvfile,encoding='utf-8')
		writer.writerow(field_names)
		writer.writerows(sql_data)
	csvfile.close()
	cursor.close()
	mydb.close()

	fp = open("./reports/"+filename,'r+')
	att = email.mime.application.MIMEApplication(fp.read(),_subtype="text/csv")
	fp.close()
	att.add_header('Content-Disposition','attachment',filename=filename)
	msg.attach(att)

	s = smtplib.SMTP('smtp.gmail.com')
	s.starttls()
	s.login('sender@domain.com','password')
	#s.sendmail('cpxi1234@gmail.com',['skrishnaswamy@cpxi.com'], msg.as_string())
	s.sendmail('sender@domain.com',recipients, msg.as_string())
	s.quit()
	print "completed the report loading process!"


if __name__=="__main__":
	access_token = access_token_retrieve()
	acc_list = accounts_list_retrieve()
	report_retrieve_db_load()
else:
	print "invalid access"




