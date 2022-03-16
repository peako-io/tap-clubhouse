#!/usr/bin/env python3

import sys
import os
import time
import datetime
from operator import itemgetter

import requests
import singer


REQUIRED_CONFIG_KEYS = ["api_token", "start_date"]
BASE_URL = "https://api.app.shortcut.com"
CONFIG = {}
STATE = {}

ENDPOINTS = {
    "stories": "/api/v3/stories/search",
    "workflows": "/api/v3/workflows",
    "members": "/api/v3/members",
    "epics": "/api/v3/epics",
    "projects": "/api/v3/projects"
}

LOGGER = singer.get_logger()
SESSION = requests.Session()


def get_url(endpoint):
    return BASE_URL + ENDPOINTS[endpoint]


@singer.utils.ratelimit(100, 60)
def request(url, params=None, data=None):
    params = params or {}

    if data:
        verb = "POST"
    else:
        verb = "GET"
        data = {}

    headers = {
        "Shortcut-Token": CONFIG["api_token"],
    }
    if "user_agent" in CONFIG:
        headers["User-Agent"] = CONFIG["user_agent"]

    req = requests.Request(verb, url, params=params, data=data, headers=headers).prepare()
    LOGGER.info("{} {}".format(verb, req.url))
    resp = SESSION.send(req)

    if "Retry-After" in resp.headers:
        retry_after = int(resp.headers["Retry-After"])
        LOGGER.info("Rate limit reached. Sleeping for {} seconds".format(retry_after))
        time.sleep(retry_after)
        return request(url, params)

    elif resp.status_code >= 400:
        LOGGER.error("{} {} [{} - {}]".format(verb, req.url, resp.status_code, resp.content))
        sys.exit(1)

    return resp


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG["start_date"]

    else:
        # Munge the date in the state due to how Clubhouse behaves. Clubhouse keeps
        # returning the same record on subsequent runs because it treats
        # `updated_at_start` as inclusive
        start = singer.utils.strptime(STATE[entity])
        STATE[entity] = singer.utils.strftime(start + datetime.timedelta(seconds=1))

    return STATE[entity]


def gen_request(entity, params=None, data=None):
    url = get_url(entity)
    params = params or {}
    data = data or {}
    rows = request(url, params, data).json()

    for row in sorted(rows, key=itemgetter("updated_at")):
        yield row


def sync_stories():
    singer.write_schema("stories", load_schema("stories"), ["id"])

    start = get_start("stories")
    data = {
        "updated_at_start": start,
    }

    for _, row in enumerate(gen_request("stories", data=data)):
        LOGGER.info("Story {}: Syncing".format(row["id"]))
        singer.utils.update_state(STATE, "stories", row["updated_at"])
        singer.write_record("stories", row)

    singer.write_state(STATE)


def sync_time_filtered(entity):
    LOGGER.info("Entity Syncing: " + entity)
    singer.write_schema(entity, load_schema(entity), ["id"])
    start = get_start(entity)

    LOGGER.info("Syncing {} from {}".format(entity, start))
    for row in gen_request(entity):
        if row["updated_at"] >= start:
            singer.utils.update_state(STATE, entity, row["updated_at"])
            singer.write_record(entity, row)

    singer.write_state(STATE)


def load_schema(entity):
    return singer.utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def do_sync():
    LOGGER.info("Starting Clubhouse sync")

    sync_stories()
    sync_time_filtered("workflows")
    sync_time_filtered("epics")
    sync_time_filtered("projects")
    sync_time_filtered("members")

    LOGGER.info("Completed sync")


def main():
    args = singer.utils.parse_args(["api_token", "start_date"])
    args.config["api_token"] = args.config["api_token"].strip()
    CONFIG.update(args.config)
    STATE.update(args.state)
    do_sync()


if __name__ == "__main__":
    main()
