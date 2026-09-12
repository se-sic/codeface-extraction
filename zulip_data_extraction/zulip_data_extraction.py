# coding=utf-8
# This file is part of codeface-extraction, which is free software: you
# can redistribute it and/or modify it under the terms of the GNU General
# Public License as published by the Free Software Foundation, version 2.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Copyright 2025-2026 by Ritika Hiremath <rihi00002@stud.uni-saarland.de>
# All Rights Reserved.
"""
This file is able to extract issue data from Zulip.
"""
import zulip
import json
import argparse
import time
import os
import sys
from codeface_utils.util import setup_logging
from logging import getLogger


# create logger
setup_logging()
log = getLogger(__name__)

# Log in to https://rust-lang.zulipchat.com, go to Personal Settings -> Account&Privacy -> API key, and download the .zuliprc file or copy the API key and utilize the template.
# Template of zuliprc.txt is present in this directory. 

# The location of zuliprc.txt file 
parser = argparse.ArgumentParser()
parser.add_argument("--zulip-config", default=None, help="Path to zuliprc config file")
parser.add_argument("--output", default=None, help="Path to the output directory") 
args = parser.parse_args(sys.argv[1:]) 

# Resolve path 
if args.zulip_config: 
    config_path = args.zulip_config 
else: 
    config_path = os.path.join(os.path.dirname(__file__), "zuliprc") 

if args.output: 
    output_path = os.path.join(args.output, "zulip.json") 
else: 
    output_path = os.path.join(os.path.dirname(__file__), "zulip.json")

# Raise error if file not found
if not os.path.exists(config_path):
    raise FileNotFoundError(f"Zulip config file not found at: {config_path}")

client = zulip.Client(config_file= config_path)


def run():
    log.info("Starting Zulip data extraction")
    # use exisiting zulip_streams_and_topics.json file 
    if os.path.exists(os.path.join(os.path.dirname(output_path), "zulip_streams_and_topics.json")):
        streams_and_topics_path = os.path.join(os.path.dirname(output_path), "zulip_streams_and_topics.json")
        with open(streams_and_topics_path, "r", encoding="utf-8") as f:
            streams_and_topics = json.load(f)

    else:
        streams_and_topics = topics_extraction()
    messages_extraction_for_each_stream(streams_and_topics)

def safe_get_topics(stream_id):
    """
    Fetches all the topics from Zulip rust.

    :params: stream_id : id of the stream.
    :return: list of topics in the stream. If no topics exist then returns empty list.

    """
    while True:
        resp = client.get_stream_topics(stream_id = stream_id)
        if resp["result"] == "success":
            return [t["name"] for t in resp["topics"]]
        elif resp["result"] == "error" and resp.get("code") == "RATE_LIMIT_HIT":
            retry = int(resp.get("retry_after", 5))
            log.info("Rate limit hit, retrying in '{}'s...".format(retry))
            time.sleep(retry)
        else:
            log.error("Error fetching topics: '{}'", resp)
            return []


def topics_extraction():
    """
    Fetches Extract all streams and topics 

    :return: returns a dictory of topics in the stream.
    """
    streams = client.get_streams()["streams"]
    data = {}

    for i, s in enumerate(streams, 1):
        stream_name = s["name"]
        log.debug(f"[{i}/{len(streams)}] Getting topics for: {stream_name}")

        topics = safe_get_topics(s["stream_id"])
        data[stream_name] = {
            "topics": topics
        }

        time.sleep(0.5)

    topics_file = os.path.join(os.path.dirname(output_path), "zulip_streams_and_topics.json")
    with open(topics_file, "w", encoding = "utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    log.info("Saved zulip_streams_and_topics.json")
    return data

     
def load_stream_topics(file_path):
    """
    Fetches all the streams and topics from the file. 

    :params: file_path : path of the file for streams and topics.
    :return: returns the content of the file.
    """
    with open(file_path, "r", encoding = "utf-8") as f:
        return json.load(f)
    


def fetch_all_messages_for_stream(stream_name,topic_name):
    """
    Fetches all the messages in a topic through API call. 

    :params: topic_name : name of the topic for which all. the messages should be fetched.
    :return: returns all the messages from that topic.
    """
    
    all_messages = []
    anchor = 0  # Start from oldest

    while True:
        request = {
            "anchor": anchor,
            "num_before": 0,
            "num_after": 500,       # max allowed
            "narrow": [
                {"operator": "stream", "operand": stream_name},
                {"operator": "topic", "operand": topic_name}
            ]
        }

        resp = client.get_messages(request)
        msgs = resp["messages"]

        if not msgs:
            break

        all_messages.extend(msgs)

        # Move anchor forward for next batch
        anchor = msgs[-1]["id"] + 1

        time.sleep(0.4)

    return all_messages



def messages_extraction_for_each_stream(streams_with_topics):
    final_output = []

    for stream_name, info in streams_with_topics.items():
        topics = info["topics"]

        for topic in topics:
            log.debug(f"\n Fetching all messages for stream: {stream_name} and topic: {topic}")
             
            msgs = fetch_all_messages_for_stream(stream_name,topic)

            for m in msgs:
                final_output.append({
                    "discussion_id": m["stream_id"],
                    "discussion_topic": m["subject"],
                    "sender_full_name": m["sender_full_name"],
                    "sender_email": m["sender_email"],
                    "stream": stream_name,
                    "content": m["content"],
                    "timestamp": m["timestamp"]
            })

    # Save everything
    with open(output_path, "w", encoding = "utf-8") as f:
        json.dump(final_output, f, indent = 2)

    log.info("\n Saved all stream messages to: '{}'".format(output_path))