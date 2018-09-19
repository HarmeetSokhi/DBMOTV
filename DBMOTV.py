#!/usr/bin/python
#****************************************************************************************
#    DBM  Python Script for  OTV
#   --------------------------------------------
#
#    Notes:
#
#    1. This script fetches data use DBM Google API .
#       You can see the variables below for the details of the API
#    2. Script take JSON as a input from the config_file
#    3. Once the data is received it writes to S3 (AWS).Please see the Variables for
#       the path details
#    4. Once Data is written to S3 it writes it to the Redshift staging table.Staging
#       table is truncated before writing the data
#    5. Data is inserted into Master table after the above step
#
#    Version:
#      Please check Bitbucket for Change Management & Details.
#
#    Author:
#      Harmeet Sokhi
#
#
#    Date :
#      21/02/2018
#*******************************************************************************************
import httplib2
import pprint
import simplejson
import time
import json
import psycopg2
import boto3
import boto
import os
import botocore
import datetime
import sys
import urllib2

from contextlib import closing

from googleapiclient.errors import HttpError
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import OAuth2Credentials
from oauth2client import GOOGLE_AUTH_URI

from oauth2client import GOOGLE_REVOKE_URI
from oauth2client import GOOGLE_TOKEN_URI



### Jenkins Passowrds  
REFRESH_TOKEN = os.environ['REFRESH_TOKEN']
CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']
PWD = os.environ['Client_Admin_DBPWD'] 

# Progaram Variables
API_NAME = "doubleclickbidmanager"
API_VERSION = "v1"
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
Config_File = 'config.json.dbm.otv'
DBNAME = 'Client'
HOST = 'REDSHIFT-URL'
PORT = 5439
USER = 'admin_Client'
SCHEMA = 'digital'
Staging_Table  = 'Client_DBM_OTV_Stg'
Master_Table =  'Client_DBM_OTV_Master'
AWS_IAM_ROLE = "aws-ec2-s3-role"
S3_FilePath = "Client/DBM/OTV/"
EC2_File_Path = "/mnt/data/DBM/Client/OTV/"
reportName = 'dbm_report_otv'
OAUTH_SCOPE = ['https://www.googleapis.com/auth/doubleclickbidmanager']

def authorization():
	try:
	        credentials = OAuth2Credentials(None, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, None,
        	                               GOOGLE_TOKEN_URI, None,
                	                       revoke_uri=GOOGLE_REVOKE_URI,
                        	               id_token=None,
                                	       token_response=None)

	        # Create an httplib2.Http object and authorize it with  credentials
	        http = httplib2.Http()
	        http = credentials.authorize(http)
		return http	
	except Exception as e:
		pprint.pprint(" >> Error in authorization: %s" %e)
		sys.exit(1)
	
def gen_report(configfile,http):
        with open(configfile,'r') as f:
             bodystr = f.read()
        bodyvar = bodystr % (startDATE_epochms,endDATE_epochms)

	print ' >> Json: ' + bodyvar
        body = simplejson.loads(bodyvar)
        SERVICE = build(API_NAME, API_VERSION, http=http)

	print ' >> Creating the Json query'
        request = SERVICE.queries().createquery(body=body)
	print ' >> Request created'

        json_data = request.execute()
	print ' >> Request executed'

	print ' >> Report generated. Query ID ' + json_data['queryId']
        return [json_data['queryId'], SERVICE]


def fetchreport(SERVICE,report_id,con):
	global reportName
	#Generate and print sample report.
	
	#  Args:
	#    service: An authorized Doublelcicksearch service.
	#    report_id: The ID DS has assigned to a report.
	#    report_fragment: The 0-based index of the file fragment from the files array.

	reportNameTemp = reportName
	if startDATE == endDATE:
		reportName = reportNameTemp + '_' + str(startDATE) + '_' + '.csv'
		reportName_x = reportNameTemp + '_x_' + str(startDATE) + '_' + '.csv'
		file_in_s3 = S3_FilePath + str(startDATE) + '/' + reportName 
	else:
		reportName = reportNameTemp + '_' + str(startDATE) + '_' + str(endDATE) + '.csv'
                reportName_x = reportNameTemp + '_x_' + str(startDATE) + '_' + str(endDATE) + '.csv'
		file_in_s3 = S3_FilePath + str(startDATE) + '...' + str(endDATE) + '/' + reportName

	repfile = EC2_File_Path + reportName
	repfile_x = EC2_File_Path + reportName_x
	for _ in xrange(50):
	   try:
	      request = SERVICE.queries().getquery(queryId=report_id)
	      query = request.execute()
              report_url = query['metadata']['googleCloudStoragePathForLatestReport']

	      if report_url <> "":
	        	print ' >> The report is ready.'
		        # Grab the report and write contents to a file.
		        print report_url
	        	with open(repfile, 'wb') as output:
	        	  with closing(urllib2.urlopen(report_url)) as url:
		            output.write(url.read())
        		print ' >> Download complete.'
			
			#Just write the individual records and remove other information 
			counter = 0
			with open(repfile) as f:
			 	with open(repfile_x, "w") as f1:
			        	for line in f:
					    if ",,,,," in line or line.strip() == '':	
						break	
					    else:
                                                f1.write(line)
                                                counter = counter + 1
		

		        f.close()
			f1.close()

        		print ' >> Filename in EC2 : ' + repfile
		        print ' >> Total records in File: ' + str(counter)

			# Grab the report and write contents to a file.
		        FilePath = write_to_s3(repfile_x,"s3bucket",file_in_s3)
		        print ' >> File written in S3. File Path is : ' + FilePath
			write_to_db(FilePath,con)
  		        return
	      else:
              	  print ' >> Report is not ready. I am trying again...'
	      	  time.sleep(30)
	   except HttpError as e:
      		error = simplejson.loads(e.content)['error']['errors'][0]
		print error
		break

def  write_to_s3(file_to_upload,bucket,file_name_in_s3):
	try:
		s3 = boto3.resource('s3')
		s3.Bucket(bucket).upload_file(file_to_upload,file_name_in_s3)
		return "s3://s3bucket/" + file_name_in_s3 
	except Exception as e:
		pprint.pprint(' >> Unable to upload to S3 : %s' %e)
		sys.exit(1)
		
def  write_to_db(FILE_PATH,con):
    try:
	print ' >> Truncating the staging table '
	deletesql = " Truncate Table " + SCHEMA + "." + Staging_Table 
	cur = con.cursor()
	# drop previous days contents from the staging table
	try:
		cur.execute(deletesql)		
		print(" >> Truncate for staging executed successfully")

	except Exception as e:
		pprint.pprint(" >> Error executing truncate in the staging table: %s " %e)
		sys.exit(1)
 
       # Copy to the staging table first and if it is successful copy to the master table

        copysql="""copy {}.{} from '{}' credentials 'aws_iam_role={}' format as csv IGNOREHEADER 1""".format(SCHEMA,Staging_Table,FILE_PATH,AWS_IAM_ROLE)

    	try:
        	cur.execute(copysql)
        	print(" >> Copy Command executed successfully")
		
		# Copy to the Master Table
		sqlMaster = " Insert into " + SCHEMA + "." + Master_Table + " Select * from " + SCHEMA + "." + Staging_Table + " STG1 where not exists(Select 1 from " + SCHEMA + "." + Master_Table + " Mas1 where MAS1.Date1 =STG1.Date1)"     	
		try:
			cur.execute(sqlMaster)
			inscount=cur.rowcount
			print ' >> Rows inserted : ' + str(inscount)
			print ' >> Data ingested successfully'
		except Exception as e:
			pprint.pprint(" >> Failed to execute Insert to Master: %s " %e)
			sys.exit(1)

    	except Exception as e:
        	pprint.pprint(" >> Failed to execute copy command. Error : %s" %e)
		sys.exit(1)
    	con.close()

    except Exception as e:
        pprint.pprint(' >> Unable to connect to Redshift.Error: %s' %e)
	sys.exit(1)

def getDates(con):

	        startDATE = os.getenv('startDATE',"")
        	endDATE = os.getenv('endDATE',"")

		if startDATE == "":
			print ' >> Calculating StartDATE '
			cur = con.cursor()
		        sqlMaster = " select max(Date1) from " + SCHEMA + "." + Master_Table 
		        try:
		            cur.execute(sqlMaster)
			    print ' >> Executed the max Date query '
             		    sqlRow = cur.fetchone()
			    print ' >> Fetching the date row,if any '
	                    if sqlRow[0] == None:
			    	startDATE = ""
			    else:
				startDATE = sqlRow[0]
			    if startDATE == "":
				print ' >> No max Date row '
				yesterDate = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
				startDATE = yesterDate
			    else:
				print ' >> setting the startDATE to next day of last updated data'
				startDATE = startDATE + datetime.timedelta(days = 1)
#	           	    print ' >> startDate is: ' + startDATE
		        except Exception as e:
		            pprint.pprint(" >> Failed to get Max Date from the Master Table %s " %e)
			    sys.exit(1)
		else:
			print ' >> Settiing startDATE from Jenkins parm'
			startDATE = os.environ['startDATE']			
			
		if endDATE == "":
			print ' >> Going to calculate endDate'
			yesterDate = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')			
			endDATE = yesterDate
		else:
			print ' >> Setting the endDATE from Jenkins parm'
			endDATE = os.environ['endDATE']

		startDATE = str(startDATE)
		endDATE = str(endDATE)

		if startDATE > endDATE:
			print ' >> startDATE greater than endDATE ?? I am setting both as endDATE'
			startDATE = endDATE
		
		print ' >> startDate : ' + startDATE 	
		print ' >> endDate: ' + endDATE
		 
		return [startDATE,endDATE]


def connectDB():
        print(" >> Establishing Connection with Redshift..")
        con=psycopg2.connect(dbname= DBNAME, host=HOST, port= PORT, user= USER, password= PWD)
        print(" >> Connection Successful!")
	return con

def main():	

	global startDATE 
	global endDATE
	global startDATE_epochms
	global endDATE_epochms

	# Get the startDATE and endDATE values from the environment Varibales in jenkins
	startDATE = os.getenv('startDATE',"")
	endDATE = os.getenv('endDATE',"")

	try:
		# Authorize thru the Access Token
		http = authorization()
		print ' >> Authorization Successful'        					
		# Connect to the redshift Database
		con = connectDB()
		print ' >> Setting Dates '
		# if the dates are not passed as parms  need to calculated 
	   	startDATE, endDATE = getDates(con)		

		# Convert Dates to epochms as DBM API takes long Date as input
		d = datetime.datetime.strptime(startDATE + ' 12:00:00,00',
                           '%Y-%m-%d %H:%M:%S,%f').strftime('%s')
		startDATE_epochms = int(d)*1000
		print ' >> startDATE epochms: ' + str(startDATE_epochms)
		print(datetime.datetime.fromtimestamp(float(d)))
                d = datetime.datetime.strptime(endDATE + ' 23:59:59,59',
                           '%Y-%m-%d %H:%M:%S,%f').strftime('%s')
		endDATE_epochms = int(d)*1000
		print ' >> endDATE epochms: ' + str(endDATE_epochms)
                print(datetime.datetime.fromtimestamp(float(d)))

		# Create the query and get the report ID
		print ' >> Setting JSON and generating report '
		report_id, SERVICE = gen_report(Config_File,http)

		# Fetch the report using the id and upload in S3 and redshift
		print ' >> Fetching Report '
		fetchreport(SERVICE, report_id,con)

	except Exception as e:
                pprint.pprint(" >> Error in the script : %s" %e)
		sys.exit(1)
			
	
if __name__ == "__main__":				
	main()