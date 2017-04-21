#!/usr/bin/env python3

import datetime
import os
import sys

import requests
import singer

from singer import utils


BASE_URL = "https://api.wootric.com/v1/"
PER_PAGE = 50
DATETIME_FMT = "%Y-%m-%d %H:%M:%S %z"
MAX_RESULT_PAGES = 30 #undocumented limit of results from API

CONFIG = {}
STATE = {}

logger = singer.get_logger()
session = requests.Session()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]


def get_start_ts(key):
    return get_ts(get_start(key))

def get_ts(isotime):
    return int(utils.strptime(isotime).timestamp())   

def get_ts_from_wootric_datestring(datestring):
    return int(datetime.datetime.strptime(datestring, DATETIME_FMT).timestamp())

def get_url(endpoint):
    return BASE_URL + endpoint


def get_access_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": CONFIG["client_id"],
        "client_secret": CONFIG["client_secret"],
    }
    resp = requests.post("https://api.wootric.com/oauth/token", data=data)
    resp.raise_for_status()
    data = resp.json()
    CONFIG["access_token"] = data["access_token"]


def gen_request(endpoint, startTs=None):
    url = BASE_URL + endpoint
    params = {
        "per_page": PER_PAGE,
        "sort_order": "asc",
        "created[gt]": get_start_ts(endpoint),
        "page": 1,
    }
    headers = {"Authorization": "Bearer {}".format(CONFIG["access_token"])}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    while True:
        req = requests.Request("GET", url, params=params, headers=headers).prepare()
        logger.info("GET {}".format(req.url))
        resp = session.send(req)
        if resp.status_code >= 400:
            logger.error("GET {} [{} - {}]".format(req.url, resp.status_code, resp.content))
            sys.exit(1)

        data = resp.json()

        for row in data:
            latest_ts = row["created_at"]
            yield row

        if len(data) == PER_PAGE:
            params["page"] += 1
            if params["page"] > MAX_RESULT_PAGES:
                #make a fresh request from our highest observed timestamp so as not to exceed the page count limit
                params["page"] = 1
                params["created[gt]"] = get_ts_from_wootric_datestring(latest_ts)
        else:
            break


def transform_datetimes(row):
    for key in ["created_at", "updated_at", "last_surveyed"]:
        if key in row and row[key] not in [None, ""]:
            dt = datetime.datetime.strptime(row[key], DATETIME_FMT)
            row[key] = utils.strftime(dt)


def sync_entity(entity):
    logger.info("Syncing {} from {}".format(entity, get_start(entity)))

    schema = load_schema(entity)
    singer.write_schema(entity, schema, ["id"])

    for i, row in enumerate(gen_request(entity)):
        transform_datetimes(row)
        singer.write_record(entity, row)
        utils.update_state(STATE, entity, row["updated_at"])
        if i % 50 == 49:
            singer.write_state(STATE)

    singer.write_state(STATE)


def do_sync():
    logger.info("Authenticating")
    get_access_token()

    sync_entity("responses")
    sync_entity("declines")
    sync_entity("end_users")

    logger.info("Completed sync")


def main():
    args = utils.parse_args(["client_id", "client_secret", "start_date"])
    CONFIG.update(args.config)

    if args.state:
        STATE.update(args.state)

    for k, v in STATE.items():
        if isinstance(v, int):
            STATE[k] = utils.strptime(v)

    do_sync()


if __name__ == '__main__':
    main()
