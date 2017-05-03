#!/usr/bin/env python
# -*- coding: utf-8 -*-


# Imports (cuz we like them on top)
import sys
import logging
import logging.handlers


#Oracle Imports
import cx_Oracle

#For Google API
import httplib2
from apiclient import errors
from apiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
from oauth2client.tools import run_flow
import argparse
from apiclient.http import BatchHttpRequest

try: import simplejson as json
except ImportError: import json

import time
import random
import copy
import timeit
from random import shuffle
import StringIO
#from auth_ggs2 import kz_d
from secrets import banHOST,banUSER,banPASS,banPORT,banSID



HOST =  'bandb-prod.sbcc.net'
PORT = 1521
SID = 'PROD'
#user = kz_d('ADIGDFlaagz1g2uDjQqLi23do0SDi3h9j11WbfQDN_Dh9NDz7A8GrEVZGVksDpkBjdU6862gXDXL')
user = banUSER
#password = kz_d('ADIGDFk3Mc4tzKhEk28IBWFqBOfUMnCpLEuVmHyiMqZVGC1Yf679EpSWr2niR7GSBUd_mtBT4P0V')
password = banPASS
#dsn_tns = cx_Oracle.makedsn(HOST, PORT, SID)
dsn_tns = cx_Oracle.makedsn(banHOST, banPORT, banSID)


def initLogging():
        global logger
        #Initialize logging
        LOG_FILENAME = 'logs/sync_class.log'
        logger = logging.getLogger('sbcc_goog_sync')
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        #Log file handler
        handler = logging.handlers.RotatingFileHandler(
                                        LOG_FILENAME, maxBytes=1024*1024, backupCount=10)


        # create formatter and add it to the handlers
        #~ formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

# Batch actions
def insert_group_batch(request_id, response, exception):
    if exception is not None:
        #Just pass it. Don't care if error
        logger.info('insert ERROR: %s %s',request_id, exception)
        pass
    else:
        #Will get to it
        resp_str = response['email']
        logger.info('SUCCESS, inserted: %s %s',request_id, resp_str)
        pass

def delete_group_batch(request_id, response, exception):
    if exception is not None:
        #Just pass it. Don't care if error
        logger.info('remove ERROR: %s %s',request_id, exception)
        pass
    else:
        #Will get to it
        #~ resp_str = response['email']
        logger.info('SUCCESS removed: %s %s',request_id, response)
        pass


def rows_to_dict_list(cursor):
    columns = [i[0] for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]

def get_classes_oracle():
	

	#~ print dsn_tns
	try:
		con = cx_Oracle.connect(user, password, dsn_tns)
	except cx_Oracle.DatabaseError as e:
		error, = e.args
		print(error.code)
		print(error.message)
		print(error.context)
		sys.exit('could not connect to Oracle Database')
		
	#Get list data
        sql = """
SELECT DISTINCT sfrstcr_crn
  || '.'
  || sfrstcr_term_code AS bom_course_id
FROM sfrstcr
WHERE
sfrstcr_term_code IN
  (SELECT GORICCR_VALUE
  FROM GORICCR
  WHERE GORICCR_ICSN_CODE='ACTIVE_TERM'
  AND GORICCR_SQPR_CODE  ='ELEARNING'
  )
AND sfrstcr_rsts_code IN
  (SELECT stvrsts_code FROM stvrsts WHERE stvrsts_voice_type = 'R'
  )
"""

		
	#~ sql = """
		#~ select group_id,bom_course_id,user_id,email_address from lp_community where bom_course_id like '%.201710'
		#~ """
	#~ sql = """
		#~ select group_id,bom_course_id,user_id,email_address from lp_community where bom_course_id in ('62091.201650','61517.201650')
		#~ """
	try:
		cur = con.cursor()
		cur.execute(sql)
		classes = rows_to_dict_list(cur)

		
		cur.close()
		con.close()
	except cx_Oracle.DatabaseError as e:
		error, = e.args
		print(error.code)
		print(error.message)
		print(error.context)
		sys.exit('Oracle Problems')
	return classes
	
def get_class_members_oracle(course):

	#~ print dsn_tns
	try:
		con = cx_Oracle.connect(user, password, dsn_tns)
	except cx_Oracle.DatabaseError as e:
		error, = e.args
		print(error.code)
		print(error.message)
		print(error.context)
		sys.exit('could not connect to Oracle Database')
		
	#Get list data
	#~ sql = """
		#~ SELECT 
		  #~ u.emailaddress
		#~ FROM lp_community_member lcm,
		  #~ user_ u,
		  #~ lp_person lp
		#~ WHERE lcm.group_id    = %d
		#~ AND lp.person_id      = lcm.user_id
		#~ AND lp.portal_user_id = u.userid
		#~ """ % course
	#~ print 'get',course
	#~ sys.exit('Bad exit: Get Course")	
	sql = """
select gzf_get_id(sfrstcr_pidm,'USERID') || '@pipeline.sbcc.edu' as EMAIL_ADDRESS
FROM sfrstcr
WHERE 
sfrstcr_term_code = '%s'
AND sfrstcr_crn         = '%s'
AND sfrstcr_rsts_code  IN
(SELECT stvrsts_code FROM stvrsts WHERE stvrsts_voice_type = 'R'
)
UNION
SELECT gzf_get_id(sirasgn_pidm,'USERID')
  || '@pipeline.sbcc.edu' AS EMAIL_ADDRESS
FROM sirasgn
WHERE --sirasgn_term_code= szf_get_this_soaterm()
sirasgn_term_code= '%s'
AND sirasgn_crn        ='%s'
		""" % (course.split('.')[1],course.split('.')[0],course.split('.')[1],course.split('.')[0])
		
	try:
		cur = con.cursor()
		cur.execute(sql)
		classe_members = set()
		for i in cur:
			classe_members.add(i[0].replace('default.com','pipeline.sbcc.edu'))
		

		
		cur.close()
		con.close()
	except cx_Oracle.DatabaseError as e:
		error, = e.args
		print(error.code)
		print(error.message)
		print(error.context)
		sys.exit('Oracle Problems')
		
	
	
	return classe_members
	


def get_service():
#All Google stuff will be put in here
	global auth_http

	try:
		flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
	except ImportError:
		flags = None

	OAUTH_SCOPE = ['https://www.googleapis.com/auth/apps.groups.settings',
									'https://www.googleapis.com/auth/admin.directory.group']
	CLIENT_SECRETS = 'lists.json'
	APPLICATION_NAME = 'Groups Settings Perm Change'
	OAUTH2_STORAGE = 'token.dat'


	# Redirect URI for installed apps
	REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

	# Run through the OAuth flow and retrieve credentials
	flow = flow_from_clientsecrets(CLIENT_SECRETS, scope=OAUTH_SCOPE)
	storage = Storage(OAUTH2_STORAGE)
	credentials = storage.get()

	# Create an httplib2.Http object and authorize it with our credentials
	if credentials is None or credentials.invalid:
			credentials = run_flow(flow, storage, flags)
	http = httplib2.Http()
	auth_http = credentials.authorize(http)

	directory_service = build('admin', 'directory_v1', http=http)
	#~ print vars(directory_service)
	gmuser_service = directory_service.members()
	return gmuser_service






	
def getGoogGroup(service,ggroup):
        groupKey = ggroup + '@pipeline.sbcc.edu'
        all_users = []
        page_token = None
        params = {'groupKey': groupKey}
        


        while True:
                try:
                        if page_token:
                                params['pageToken'] = page_token
                        current_page = service.list(**params).execute()
                        all_users.extend(current_page['members'])
                        page_token = current_page.get('nextPageToken')
                        if not page_token:
                                break

                except errors.HttpError as error:
                        print 'An error occurred: %s' % error
                        break

        user = set()
        for i in all_users:
			user.add(i['email'])
			
        return user




def main(args):
	start_time = timeit.default_timer()
	initLogging()
	logger.info('Start group sync processing')
	
	tomanny = 0
	tolittle = 0
	# Get the google service
	goog_service = get_service()
	#~ Batch services, bypassing exponential backoff
	
	#~ print goog_service
	
	#~ Get Classes
	logger.info("Getting Classes from Oracle")
	sbcc_classes = get_classes_oracle()
	#~ print len(sbcc_classes)
	#So that they are not done in the same order
	shuffle(sbcc_classes)
	#~ print len(sbcc_classes)
	#~ sys.exit()

	logger.info(sbcc_classes)
	for course in sbcc_classes:
		logger.info("Processing %s", course['BOM_COURSE_ID'])
		#~ print course
		course['MEMBERS'] = get_class_members_oracle(course['BOM_COURSE_ID'])
		
		logger.info("\nGoogle Groups:")
		try:
			goog_members = set()
			goog_members = getGoogGroup(goog_service,course['BOM_COURSE_ID'])
		except:
			logger.info("Could not get group members for: %s", course['BOM_COURSE_ID'])
			pass
		#~ print
		tosend = course['MEMBERS'] - goog_members
				
		batch = BatchHttpRequest(callback=insert_group_batch)
		if tosend:
			tomanny += 1	
			logger.info("Adding users to: %s",course['BOM_COURSE_ID']+'@pipeline.sbcc.edu')
			for i in tosend:
				logger.info("Adding: %s", i)
				member_body = { "email": i}
				batch.add(goog_service.insert(groupKey=course['BOM_COURSE_ID']+'@pipeline.sbcc.edu', body = member_body))
			logger.info("Sending to adds for %s to Google",course['BOM_COURSE_ID']+'@pipeline.sbcc.edu')
			try:
				batch.execute(http=auth_http)
			except:
				logger.info("Failed to update: %s", course['BOM_COURSE_ID']+'@pipeline.sbcc.edu')
				
				
		#~ print "Delete People"
		batch = BatchHttpRequest(callback=delete_group_batch)
		tosend = goog_members - course['MEMBERS']
		if tosend:
			tolittle += 1
			logger.info("Deleting users to: %s",course['BOM_COURSE_ID'])
			#~ print tosend
			for i in tosend:
				logger.info("Deleting: %s", i)
				batch.add(goog_service.delete(groupKey=course['BOM_COURSE_ID']+'@pipeline.sbcc.edu', memberKey = i))
			logger.info("Sending to deletes for %s to Google",course['BOM_COURSE_ID'])
			try:
				batch.execute(http=auth_http)
			except:
				logger.info("Failed to update: %s", course['BOM_COURSE_ID'])
					
		
		
		

	logger.info('End group sync processing')
	logger.info('Classes that required students to be added: %s', tomanny)
	logger.info('Classes that required students to be removed: %s', tolittle)
	timetorun = timeit.default_timer() - start_time
	logger.info('Total run time: %s', timetorun)
	
	return 0
	
	

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
