import datetime
import json
import logging
import os

import pymongo
from pymongo import MongoClient

MONGO_URL_SECRET_ARN = os.getenv('MONGO_URL_SECRET_ARN')
MONGO_DB = os.getenv('MONGO_DB_NAME', 'avrae')
MONGO_URL_OVERRIDE = os.getenv('MONGO_URL')  # for manual running

# init logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_mongo_url():
    if MONGO_URL_OVERRIDE is not None:
        return MONGO_URL_OVERRIDE
    import boto3
    session = boto3.session.Session()
    secrets_client = session.client('secretsmanager')
    get_secret_value_response = secrets_client.get_secret_value(
        SecretId=MONGO_URL_SECRET_ARN
    )
    return get_secret_value_response['SecretString']


# init mongo
MONGO_URL = get_mongo_url()
client = MongoClient(MONGO_URL)
db = client[MONGO_DB]


# helpers
def get_statistic(key):
    try:
        value = int(
            db.random_stats.find_one({"key": key}).get("value", 0)
        )
    except AttributeError:
        value = 0
    return value


# calculators

# each of these return (total_today, total_life) [dict<int, int>, dict<int, int>]
def calculate_command_activity(last_to_date):
    commands_to_date = {}
    last_commands_to_date = last_to_date.get("command_activity", {})
    for doc in db.analytics_command_activity.find():
        commands_to_date[doc['name']] = doc['num_invocations']

    commands_today = {}
    for command, to_date in commands_to_date.items():
        commands_today[command] = to_date - last_commands_to_date.get(command, 0)

    return commands_today, commands_to_date


# each of these should return (total_today, total_life) [int, int]
def calculate_num_commands(last_to_date):
    num_commands_now = get_statistic("commands_used_life")
    delta = num_commands_now - last_to_date.get("num_commands", 0)
    return delta, num_commands_now


def calculate_num_characters(last_to_date):
    num_characters_now = db.characters.estimated_document_count()
    delta = num_characters_now - last_to_date.get("num_characters", 0)
    return delta, num_characters_now


# returns {day: int, week: int, month: int}
def calculate_num_active_users(now):
    out = {
        "day": db.analytics_user_activity.count_documents({"last_command_time":
                                                               {"$gt": now - datetime.timedelta(days=1),
                                                                "$lte": now}}),
        "week": db.analytics_user_activity.count_documents({"last_command_time":
                                                                {"$gt": now - datetime.timedelta(days=7),
                                                                 "$lte": now}}),
        "month": db.analytics_user_activity.count_documents({"last_command_time":
                                                                 {"$gt": now - datetime.timedelta(days=30),
                                                                  "$lte": now}})
    }
    return out


def calculate_num_active_guilds(now):
    out = {
        "day": db.analytics_guild_activity.count_documents({"last_command_time":
                                                                {"$gt": now - datetime.timedelta(days=1),
                                                                 "$lte": now}}),
        "week": db.analytics_guild_activity.count_documents({"last_command_time":
                                                                 {"$gt": now - datetime.timedelta(days=7),
                                                                  "$lte": now}}),
        "month": db.analytics_guild_activity.count_documents({"last_command_time":
                                                                  {"$gt": now - datetime.timedelta(days=30),
                                                                   "$lte": now}})
    }
    return out


def calculate_alias_calls(now, event_type):
    out = {
        "day": db.analytics_alias_events.count_documents(
            {"type": event_type,
             "timestamp": {"$gt": now - datetime.timedelta(days=1),
                           "$lte": now}}
        ),
        "week": db.analytics_alias_events.count_documents(
            {"type": event_type,
             "timestamp": {"$gt": now - datetime.timedelta(days=7),
                           "$lte": now}}
        ),
        "month": db.analytics_alias_events.count_documents(
            {"type": event_type,
             "timestamp": {"$gt": now - datetime.timedelta(days=30),
                           "$lte": now}}
        ),
        "to_date": db.analytics_alias_events.count_documents(
            {"type": event_type,
             "timestamp": {"$lte": now}}
        )
    }
    return out


# main
def calculate_daily(now=None):
    now = now or datetime.datetime.now()
    try:
        last = next(db.analytics_daily.find().sort("timestamp", pymongo.DESCENDING).limit(1))
    except StopIteration:
        last = {}
    last_to_date = last.get("to_date", {})

    # setup
    out = {"timestamp": now}
    to_date = dict()

    # --- calculations ---

    # -- deltas --
    # most popular commands today
    # fixme: how does this work in a columnal data store?
    # out['command_activity'], to_date['command_activity'] = calculate_command_activity(last_to_date)

    # commands called today
    out['num_commands'], to_date['num_commands'] = calculate_num_commands(last_to_date)
    # characters imported today
    out['num_characters'], to_date['num_characters'] = calculate_num_characters(last_to_date)

    # -- timeframed --
    # users active today/this week/this month (have called a command in the last 24h/1w/1mo)
    out['num_active_users'] = calculate_num_active_users(now)
    # guilds active today/this week/this month (have called a command in the last 24h/1w/1mo)
    out['num_active_guilds'] = calculate_num_active_guilds(now)

    # -- alias stats --
    # {
    #   "day": number,
    #   "week": number,
    #   "month": number,
    #   "to_date": number
    # }

    # aliases called today
    out['num_alias_calls'] = calculate_alias_calls(now, "alias")
    # servaliases called today
    out['num_servalias_calls'] = calculate_alias_calls(now, "servalias")
    # snippets called today
    out['num_snippet_calls'] = calculate_alias_calls(now, "snippet")
    # servsnippets called today
    out['num_servsnippet_calls'] = calculate_alias_calls(now, "servsnippet")
    # workshop aliases called today
    out['num_workshop_alias_calls'] = calculate_alias_calls(now, "workshop_alias")
    # workshop servaliases called today
    out['num_workshop_servalias_calls'] = calculate_alias_calls(now, "workshop_servalias")
    # workshop snippets called today
    out['num_workshop_snippet_calls'] = calculate_alias_calls(now, "workshop_snippet")
    # workshop servsnippets called today
    out['num_workshop_servsnippet_calls'] = calculate_alias_calls(now, "workshop_servsnippet")

    # to date, for delta calcs
    out['to_date'] = to_date

    return out


def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))

    db.analytics_daily.insert_one(calculate_daily())

    logger.info("Done!")


if __name__ == '__main__':
    days = []
    db.analytics_daily.delete_many(
        {"timestamp": {"$gte": days[0], "$lte": days[-1]}}
    )
    for day in days:
        print(day)
        db.analytics_daily.insert_one(calculate_daily(day))

    logger.info("Done!")
