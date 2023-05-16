#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

# Imports (cuz we like them on top)
import sys

from pathlib import Path
from loguru import logger

LOG_FILENAME = Path("logs/sync_class.log")
logger.remove()
logger.add(LOG_FILENAME, rotation="1 day", retention="14 days")


# Oracle Imports
import cx_Oracle

# For Google API
# import httplib2
# from apiclient import errors
# from apiclient.discovery import build
# from oauth2client.client import flow_from_clientsecrets
# from oauth2client.file import Storage
# from oauth2client import tools
# from oauth2client.tools import run_flow
# import argparse
# from apiclient.http import BatchHttpRequest
from googleapiclient.http import BatchHttpRequest
from googleapiclient.errors import HttpError

# try: import simplejson as json
# except ImportError: import json


import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import time
import random
import copy
import timeit
from random import shuffle


# from auth_ggs2 import kz_d
# from secrets import banHOST,banUSER,banPASS,banPORT,banSID


HOST = ""
PORT = 
SID = ""


@logger.catch
def setSettings(groupId, service):
    group = service.groups()
    # g = group.get(groupUniqueId=groupId).execute()
    # print("\nGroup properties for group %s\n" % g["name"])
    # pprint.pprint(g)
    body = {}
    body["whoCanPostMessage"] = "ALL_MEMBERS_CAN_POST"
    # update group
    g1 = group.update(groupUniqueId=groupId, body=body).execute()
    # print("\nUpdated Access Permissions to the group\n")
    # pprint.pprint(g1)
    logger.info(f'Updated Access Permissions to the group {g1["name"]}')


# nonbatch
@logger.catch
def insert_member_google(service, group_member, ggroup):
    # Sends the update to google.
    # ~ emailaddress = group_member
    member_body = {"email": group_member}
    # ~ print member_body
    results = service.members().insert(groupKey=ggroup, body=member_body).execute()


@logger.catch
def delete_member_google(service, group_member, ggroup):
    # Sends the update to google.
    # ~ member_body = { "email": group_member + '@pipeline.sbcc.edu'}
    # ~ print member_body
    # member_body = { "email": group_member}
    member_body = group_member
    results = service.members().delete(groupKey=ggroup, memberKey=member_body).execute()


# Batch actions
@logger.catch
def insert_group_batch(request_id, response, exception):
    if exception is not None:
        # Just pass it. Don't care if error
        logger.error(f"insert ERROR: {request} {exception}")
        pass
    else:
        # Will get to it
        resp_str = response["email"]
        logger.success(f"SUCCESS, inserted: {request_id} {resp_str}")
        pass


@logger.catch
def delete_group_batch(request_id, response, exception):
    if exception is not None:
        # Just pass it. Don't care if error
        logger.error(f"remove ERROR: {request_id} {exception}")
        pass
    else:
        # Will get to it
        # ~ resp_str = response['email']
        logger.success(f"SUCCESS removed: {request_id} {response}")
        pass


@logger.catch
def rows_to_dict_list(cursor):
    columns = [i[0] for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]


@logger.catch
def get_classes_oracle():
    # ~ print dsn_tns
    try:
        # con = cx_Oracle.connect(user, password, dsn_tns)
        con = cx_Oracle.connect(dsn="lsrvprod", encoding="UTF-8")
    except cx_Oracle.DatabaseError as e:
        (error,) = e.args
        logger.error(f"{error.code}")
        logger.error(f"{error.message}")
        logger.error(f"{error.context}")
        sys.exit("could not connect to Oracle Database")

    # Get list data
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

    sql = """
SELECT DISTINCT sfrstcr_crn
  || '.'
  || sfrstcr_term_code AS bom_course_id
FROM sfrstcr
WHERE
sfrstcr_term_code in (  '202410','202430' )
AND sfrstcr_rsts_code IN
  (SELECT stvrsts_code FROM stvrsts WHERE stvrsts_voice_type = 'R'
  )
"""

    # ~ sql = """
    # ~ select group_id,bom_course_id,user_id,email_address from lp_community where bom_course_id like '%.201710'
    # ~ """
    # ~ sql = """
    # ~ select group_id,bom_course_id,user_id,email_address from lp_community where bom_course_id in ('62091.201650','61517.201650')
    # ~ """
    try:
        cur = con.cursor()
        cur.execute(sql)
        classes = rows_to_dict_list(cur)

        cur.close()
        con.close()
    except cx_Oracle.DatabaseError as e:
        (error,) = e.args
        logger.error(f"{error.code}")
        logger.error(f"{error.message}")
        logger.error(f"{error.context}")
        sys.exit("Oracle Problems")
    return classes


@logger.catch
def get_class_members_oracle(course):
    # ~ print dsn_tns
    try:
        con = cx_Oracle.connect(dsn="lsrvprod", encoding="UTF-8")
    except cx_Oracle.DatabaseError as e:
        (error,) = e.args
        logger.error(f"{error.code}")
        logger.error(f"{error.message}")
        logger.error(error.context)
        sys.exit("could not connect to Oracle Database")

    # Get list data
    # ~ sql = """
    # ~ SELECT
    # ~ u.emailaddress
    # ~ FROM lp_community_member lcm,
    # ~ user_ u,
    # ~ lp_person lp
    # ~ WHERE lcm.group_id    = %d
    # ~ AND lp.person_id      = lcm.user_id
    # ~ AND lp.portal_user_id = u.userid
    # ~ """ % course
    # ~ print 'get',course
    # ~ sys.exit('Bad exit: Get Course")
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
WHERE 
sirasgn_term_code= '%s'
AND sirasgn_crn        ='%s'
		""" % (
        course.split(".")[1],
        course.split(".")[0],
        course.split(".")[1],
        course.split(".")[0],
    )

    try:
        cur = con.cursor()
        cur.execute(sql)
        classe_members = set()
        for i in cur:
            classe_members.add(i[0].replace("default.com", "pipeline.sbcc.edu"))

        cur.close()
        con.close()
    except cx_Oracle.DatabaseError as e:
        (error,) = e.args
        logger.error(f"{error.code}")
        logger.error(f"{error.message}")
        logger.error(f"{error.context}")
        sys.exit("Oracle Problems")

    return classe_members


@logger.catch
def get_service():
    SCOPES = [
        "https://www.googleapis.com/auth/admin.directory.user",
        "https://www.googleapis.com/auth/apps.groups.settings",
        "https://www.googleapis.com/auth/admin.directory.group",
    ]
    workDir = Path(__file__).resolve().parent.parent
    #               if not os.path.exists(credential_dir):
    #                       os.makedirs(credential_dir)
    # credential_path = os.path.join(credential_dir,'admin-directory_v1-python-quickstart.json')
    key_file = workDir / "credentials.json"
    token_file = workDir / "token.pickle"
    logger.info(f"{token_file}")
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_file):
        with open(token_file, "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(key_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        logger.info(f"Updating {token_file}")
        with open(token_file, "wb") as token:
            pickle.dump(creds, token)

    directory_service = build("admin", "directory_v1", credentials=creds)
    settings_service = build("groupssettings", "v1", credentials=creds)
    # for group settings

    # ~ print vars(directory_service)
    # ~ gmuser_service = directory_service.members()
    logger.info(f"Directory Service v1: {directory_service}")
    dir_info = dir(directory_service)
    logger.info(f"{dir_info}")
    # sys.exit("Must check Creds")
    return directory_service, settings_service


@logger.catch
def setSettings(groupId, service):
    group = service.groups()
    # g = group.get(groupUniqueId=groupId).execute()
    # print("\nGroup properties for group %s\n" % g["name"])
    # pprint.pprint(g)
    body = {}
    body["whoCanPostMessage"] = "ALL_MEMBERS_CAN_POST"
    # update group
    g1 = group.update(groupUniqueId=groupId, body=body).execute()
    # print("\nUpdated Access Permissions to the group\n")
    # pprint.pprint(g1)
    logger.info(f'Updated Access Permissions to the group {g1["email"]}')


@logger.catch
def createGoogGroup(service, serv_setting, ggroup):
    groupKey = ggroup["BOM_COURSE_ID"] + "@pipeline.sbcc.edu"
    course_name = ggroup["COURSE_NAME"]
    description = ggroup["BOM_COURSE_ID"] + " " + course_name
    params = {"email": groupKey, "name": groupKey}

    logger.info(f"Creating Group: {params}")
    try:
        api_result = service.groups().insert(body=params).execute()
        logger.info(f"Group Created {api_result}")
        setSettings(groupKey, serv_setting)
    except Exception as error:
        logger.error(f"An error occurred creating group: {error}")
    # sys.exit("Stop Create Group")


@logger.catch
def getGoogGroup(service, set_service, ggroup):
    groupKey = ggroup + "@pipeline.sbcc.edu"
    all_users = []
    page_token = None
    params = {"groupKey": groupKey}

    while True:
        try:
            if page_token:
                params["pageToken"] = page_token
            current_page = service.members().list(**params).execute()
            all_users.extend(current_page["members"])
            page_token = current_page.get("nextPageToken")
            if not page_token:
                break

        except Exception as e:
            reason = e._get_reason()
            logger.error(f"An error occurred: {reason}")
            if "groupKey" in reason:
                logger.error("An error occurred: Cannot get group")
                createGoogGroup(service, set_service, ggroup)
            # sys.exit()
            # raise
            break
        else:
            break

    user = set()
    for i in all_users:
        user.add(i["email"])

    return user


def main(args):
    start_time = timeit.default_timer()
    # initLogging()
    logger.info("Start group sync processing")

    tomanny = 0
    tolittle = 0
    # Get the google service
    goog_service, goog_settings = get_service()
    # ~ Batch services, bypassing exponential backoff

    # ~ print goog_service

    # ~ Get Classes
    logger.info("Getting Classes from Oracle")
    sbcc_classes = get_classes_oracle()
    # sbcc_classes = [{'BOM_COURSE_ID':'33818.202130'},{'BOM_COURSE_ID':'42044.202130'}]
    # ~ print len(sbcc_classes)
    # So that they are not done in the same order
    shuffle(sbcc_classes)
    logger.info(f"Classess to process: {len(sbcc_classes)}")
    # ~ sys.exit()
    for course in sbcc_classes:
        # logger.info('Classes to Process')
        logger.info(f"{course}")

    for course in sbcc_classes:
        logger.info(f"Processing {course['BOM_COURSE_ID']}")
        # ~ print course
        course["MEMBERS"] = get_class_members_oracle(course["BOM_COURSE_ID"])

        logger.info("\nGoogle Groups:")
        try:
            goog_members = set()
            goog_members = getGoogGroup(
                goog_service, goog_settings, course["BOM_COURSE_ID"]
            )
        except:
            logger.error(f"Could not get group members for: {course['BOM_COURSE_ID']}")
            createGoogGroup(goog_service, goog_settings, course)
            pass
        # ~ print
        try:
            tosend = course["MEMBERS"] - goog_members

            # batch = BatchHttpRequest(callback=insert_group_batch)
            if tosend:
                tomanny += 1
                logger.info(
                    f"Adding users to: {course['BOM_COURSE_ID']+'@pipeline.sbcc.edu'}"
                )
                for i in tosend:
                    try:
                        logger.info(f"Adding: {i}")
                        member_body = {"email": i}
                        insert_member_google(
                            goog_service,
                            i,
                            course["BOM_COURSE_ID"] + "@pipeline.sbcc.edu",
                        )
                    except Exception as e:
                        reason = e._get_reason()
                        logger.error(f"Could not add: {i}")
                        logger.error(f"{reason}")
                        # Create the group
                        if reason == "notFound":
                            createGoogGroup(goog_service, goog_settings, course)
                            break
        except Exception as e:
            logger.error(f"Bounced out of adding users: {e}")
            pass

        # ~ print "Delete People"
        batch = BatchHttpRequest(callback=delete_group_batch)
        try:
            tosend = goog_members - course["MEMBERS"]
            if tosend:
                tolittle += 1
                logger.info(f"Deleting users to: {course['BOM_COURSE_ID']}")
                # ~ print tosend
                for i in tosend:
                    logger.info(f"Deleting: {i}")
                    delete_member_google(
                        goog_service, i, course["BOM_COURSE_ID"] + "@pipeline.sbcc.edu"
                    )
        except Exception as e:
            logger.error(f"Bounced out of deleting users: {e}")
            pass

    logger.info("End group sync processing")
    logger.info(f"Classes that required students to be added: {tomanny}")
    logger.info(f"Classes that required students to be removed: {tolittle}")
    timetorun = timeit.default_timer() - start_time
    logger.info(f"Total run time: {timetorun}")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
