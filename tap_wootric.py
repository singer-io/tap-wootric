#!/usr/bin/env python3

import os
import argparse
import logging
import requests
import stitchstream as ss
import sys
import json
import datetime

session = requests.Session()
logger = ss.get_logger()
state = {}

def authed_get(url):
    return session.request(method='get', url=url)

def authed_get_all_pages(baseUrl, bookmarkName):
    global state
    while True:
        url = baseUrl
        if state.get(bookmarkName, None):
            url = baseUrl + '&created[gt]=' + state[bookmarkName]
        r = authed_get(url)
        rJson = r.json();
        yield r
        if len(rJson) <= 1:
            break

def wootricdate_to_datetime(wootricDateString):
    return datetime.datetime.strptime(wootricDateString, '%Y-%m-%d %H:%M:%S %z')


response_schema = {'type': 'object',
                 'properties': {
                     'id': {
                         'type': 'integer',
                     },
                     'created_at': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'updated_at': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'score': {
                         'type': 'integer'
                     },
                     'text': {
                         'type': ['string','null']
                     },
                     'ip_address': {
                         'type': 'string'
                     },
                     'origin_url': {
                         'type': 'string'
                     },
                     'end_user_id': {
                         'type': 'integer'
                     },
                     'survey_id': {
                         'type': 'integer'
                     },
                     'completed': {
                         'type': 'boolean'
                     },
                     'excluded_from_calculations': {
                         'type': 'boolean'
                     },
                     'tags': {
                         'type': 'array',
                         'items': {
                             "type": "string"
                         }
                     },
                 },
                 'required': ['id']
             }

def get_all_new_responses():
    global state
    
    last_response_unixtime = None
    requestUrl = 'https://api.wootric.com/v1/responses?per_page=50&sort_order=asc'
    for apiResponse in authed_get_all_pages(requestUrl, 'responses'):
        responses = apiResponse.json()
        if len(responses) > 0:
            last_created_at_datetime = wootricdate_to_datetime(responses[-1]['created_at'])
            last_response_unixtime = int(last_created_at_datetime.timestamp())

        for index, item in enumerate(responses):
            responses[index]['created_at'] = wootricdate_to_datetime(responses[index]['created_at']).isoformat()
            responses[index]['updated_at'] = wootricdate_to_datetime(responses[index]['updated_at']).isoformat()

        ss.write_records('responses', responses)

        #there is a limitation of wootric's API that only allows you to get 50 records at a time and has
        #no pagination trigger other than created_at date; as such if >50 records have the same created_at
        #date you hit an infinite loop of requests; this breaks you out of that loop if it happens
        if state.get('responses', None) == str(last_response_unixtime) and len(responses) > 1:
            logger.error('Breaking retrieval loop for responses at unixtime ' + str(last_response_unixtime) + ', will cause missing data')
            last_response_unixtime = last_response_unixtime + 1

        if last_response_unixtime: #can be none if no new responses
            state['responses'] = str(last_response_unixtime)


decline_schema = {'type': 'object',
                 'properties': {
                     'id': {
                         'type': 'integer',
                     },
                     'created_at': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'updated_at': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'end_user_id': {
                         'type': 'integer'
                     },
                     'survey_id': {
                         'type': 'integer'
                     }
                 },
                 'required': ['id']
             }

def get_all_new_declines():
    global state
    
    last_decline_unixtime = None
    requestUrl = 'https://api.wootric.com/v1/declines?per_page=50&sort_order=asc'
    for response in authed_get_all_pages(requestUrl, 'declines'):
        declines = response.json()
        if len(declines) > 0:
            last_created_at_datetime = wootricdate_to_datetime(declines[-1]['created_at'])
            last_decline_unixtime = int(last_created_at_datetime.timestamp())

        for index, item in enumerate(declines):
            declines[index]['created_at'] = wootricdate_to_datetime(declines[index]['created_at']).isoformat()
            declines[index]['updated_at'] = wootricdate_to_datetime(declines[index]['updated_at']).isoformat()

        ss.write_records('declines', declines)
        
        #there is a limitation of wootric's API that only allows you to get 50 records at a time and has
        #no pagination trigger other than created_at date; as such if >50 records have the same created_at
        #date you hit an infinite loop of requests; this breaks you out of that loop if it happens
        if state.get('declines', None) == str(last_decline_unixtime) and len(declines) > 1:
            logger.error('Breaking retrieval loop for declines at unixtime ' + str(last_decline_unixtime) + ', will cause missing data')
            last_decline_unixtime = last_decline_unixtime + 1

        if last_decline_unixtime: #can be None if no new declines
            state['declines'] = str(last_decline_unixtime)

enduser_schema = {'type': 'object',
                 'properties': {
                     'id': {
                         'type': 'integer',
                     },
                     'created_at': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'updated_at': {
                         'type': 'string',
                         'format': 'date-time'
                     },
                     'email': {
                         'type': 'string'
                     },
                     'last_surveyed': {
                         "anyOf": [
                             {
                                 "type": "null",
                             }, 
                             {
                                 "type": "string",
                                 'format': 'date-time'
                             }
                         ]
                     },
                     'external_created_at': {
                         'type': ['integer','null']
                     },
                     'page_views_count': {
                         'type': 'integer'
                     }
                 },
                 'required': ['id']
             }

def get_all_new_endusers():
    global state
    
    last_enduser_unixtime = None
    requestUrl = 'https://api.wootric.com/v1/end_users?per_page=50&sort_order=asc'
    for response in authed_get_all_pages(requestUrl, 'endusers'):
        endusers = response.json()
        if len(endusers) > 0:
            last_created_at_datetime = wootricdate_to_datetime(endusers[-1]['created_at'])
            last_enduser_unixtime = int(last_created_at_datetime.timestamp())

        for index, item in enumerate(endusers):
            endusers[index]['created_at'] = wootricdate_to_datetime(endusers[index]['created_at']).isoformat()
            endusers[index]['updated_at'] = wootricdate_to_datetime(endusers[index]['updated_at']).isoformat()
            if endusers[index]['last_surveyed']:
                endusers[index]['last_surveyed'] = wootricdate_to_datetime(endusers[index]['last_surveyed']).isoformat()

        ss.write_records('endusers', endusers)

        #there is a limitation of wootric's API that only allows you to get 50 records at a time and has
        #no pagination trigger other than created_at date; as such if >50 records have the same created_at
        #date you hit an infinite loop of requests; this breaks you out of that loop if it happens
        if state.get('endusers', None) == str(last_enduser_unixtime) and len(endusers) > 1:
            logger.error('Breaking retrieval loop for enduers at unixtime ' + str(last_enduser_unixtime) + ', will cause missing data')
            last_enduser_unixtime = last_enduser_unixtime + 1

        if last_enduser_unixtime: #can be None if no new endusers
            state['endusers'] = str(last_enduser_unixtime)

def get_access_token(client_id, client_secret):
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    response = requests.post('https://api.wootric.com/oauth/token', data=data).json();
    if 'access_token' in response:
        return response['access_token']
    raise Exception('Access Token Retrieval Failed: ' + str(response))


def do_sync(args):
    global state
    with open(args.config) as config_file:
        config = json.load(config_file)

    missing_keys = []
    for key in ['client_id', 'client_secret']:
        if key not in config:
            missing_keys += [key]

    if len(missing_keys) > 0:
        logger.fatal("Missing required configuration keys: {}".format(missing_keys))

    access_token = get_access_token(config['client_id'], config['client_secret'])
    session.headers.update({'authorization': 'Bearer ' + access_token})

    if args.state:
        with open(args.state, 'r') as file:
            for line in file:
                state = json.loads(line.strip())

    if state.get('endusers', None):
        logger.info('Replicating endusers since %s', state.get('endusers', None))
    else:
        logger.info('Replicating all endusers')
    ss.write_schema('endusers', enduser_schema, 'id')
    get_all_new_endusers()

    if state.get('responses', None):
        logger.info('Replicating responses since %s', state.get('responses', None))
    else:
        logger.info('Replicating all responses')
    ss.write_schema('responses', response_schema, 'id')
    get_all_new_responses()

    if state.get('declines', None):
        logger.info('Replicating declines since %s', state.get('declines', None))
    else:
        logger.info('Replicating all declines')
    ss.write_schema('declines', decline_schema, 'id')
    get_all_new_declines()

    ss.write_state(state)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    do_sync(args)

    
if __name__ == '__main__':
    main()
