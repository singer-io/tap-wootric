#!/usr/bin/env python3

import datetime
import os
from datetime import timezone

import backoff
import requests
import singer
from singer import utils

BASE_URL = "https://api.wootric.com/v1/"
PER_PAGE = 50
SLIDING_WINDOW = 86400  # 86400 = 1 day in seconds
DATETIME_FMT = "%Y-%m-%d %H:%M:%S %z"

CONFIG = {}
STATE = {}

logger = singer.get_logger()
session = requests.Session()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def get_start(key):
    if key not in STATE or key != "end_users":
        return CONFIG['start_date']

    return STATE[key]


def get_start_ts(key):
    return int(utils.strptime(get_start(key)).timestamp())


def get_update_start_ts(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return int(utils.strptime(STATE[key]).timestamp())


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


def giveup_condition(e):
    return e.response is not None and 400 <= e.response.status_code < 500 and not e.response.status_code == 429


@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=7,
                      jitter=None,
                      giveup=giveup_condition,
                      factor=2)
def request(url, params):
    headers = {"Authorization": "Bearer {}".format(CONFIG["access_token"])}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request("GET", url, params=params, headers=headers).prepare()
    logger.info("GET {}".format(req.url))
    resp = session.send(req)
    if resp.status_code >= 400:
        logger.error("GET {}: [{} - {}]".format(req.url, resp.status_code, resp.content))
    resp.raise_for_status()

    logger.info("X-Rate-Limit-Limit: {}".format(resp.headers.get("X-Rate-Limit-Limit")))
    logger.info("X-Rate-Limit-Remaining: {}".format(resp.headers.get("X-Rate-Limit-Remaining")))

    return resp


def gen_request(endpoint):
    url = BASE_URL + endpoint

    supports_updated = endpoint == "end_users"
    query_key_gt = "updated[gt]" if supports_updated else "created[gt]"
    query_key_lt = "updated[lt]" if supports_updated else "created[lt]"
    sort_key = "updated_at" if supports_updated else "created_at"

    sliding_window = SLIDING_WINDOW if endpoint == "end_users" else SLIDING_WINDOW * 30

    params = {
        "per_page": PER_PAGE,
        "sort_order": "asc",
        query_key_gt: get_start_ts(endpoint),
        query_key_lt: get_start_ts(endpoint) + sliding_window,
        "page": 1,
        # sort_key is not a documented feature in the Wootric API. The CTO
        # told us about it after we reached out about some issues we were
        # seeing.
        "sort_key": sort_key
    }

    last_date = params[query_key_gt]
    last_round = False
    sync_start = datetime.datetime.utcnow()

    last_bookmark = get_update_start_ts(endpoint)

    while True:
        resp = request(url, params)
        if not resp:
            break

        data = resp.json()
        for row in data:
            last_date = int(
                datetime.datetime.strptime(row[sort_key], DATETIME_FMT).astimezone(timezone.utc).timestamp())
            last_updated_at = int(
                datetime.datetime.strptime(row["updated_at"], DATETIME_FMT).astimezone(timezone.utc).timestamp())
            if last_updated_at > last_bookmark:
                yield row

        # The Wootric API won't let us fetch more than 30 pages. We'll get
        # an error if we do.
        if params["page"] >= 30:
            params["page"] = 1
            if len(data) == PER_PAGE:
                params[query_key_gt] = last_date
            else:
                params[query_key_gt] = params[query_key_lt] - 1  # [lt] and [gt] are not inclusive
                params[query_key_lt] = params[query_key_gt] + sliding_window
        elif len(data) == 0:
            params["page"] = 1
            params[query_key_gt] = params[query_key_lt] - 1  # [lt] and [gt] are not inclusive
            params[query_key_lt] = params[query_key_gt] + sliding_window
        else:
            params["page"] += 1

        if last_round and len(data) == 0:
            break

        if params[query_key_lt] > sync_start.timestamp():
            last_round = True

    STATE[endpoint] = utils.strftime(sync_start)


def transform_datetimes(row):
    for key in ["created_at", "updated_at", "last_surveyed"]:
        if key in row and row[key] not in [None, ""]:
            dt = datetime.datetime.strptime(row[key], DATETIME_FMT)
            row[key] = utils.strftime(dt.astimezone(timezone.utc))


def sync_entity(entity):
    logger.info("Syncing {} from {}".format(entity, get_start(entity)))

    schema = load_schema(entity)
    singer.write_schema(entity, schema, ["id"])

    for i, row in enumerate(gen_request(entity)):
        transform_datetimes(row)
        singer.write_record(entity, row)
        utils.update_state(STATE, entity, row["updated_at"])

        # "end_users" is the only one that can be queried by updated_at
        # As such, the other streams require a full sync before writing bookmarks.
        if i % 50 == 49 and entity == "end_users":
            singer.write_state(STATE)

    singer.write_state(STATE)


def do_sync():
    logger.info("Authenticating")
    get_access_token()

    sync_entity("responses")
    sync_entity("declines")
    sync_entity("end_users")

    logger.info("Completed sync")


def main_impl():
    args = utils.parse_args(["client_id", "client_secret", "start_date"])
    CONFIG.update(args.config)

    if args.state:
        STATE.update(args.state)

    do_sync()


def main():
    try:
        main_impl()
    except Exception as exc:
        logger.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
