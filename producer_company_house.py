#!/usr/bin/env python

import sys
from random import choice
import json
import requests
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

if __name__ == '__main__':

    url = "https://stream.companieshouse.gov.uk/"

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Set idempotence configuration
    config['enable.idempotence'] = 'true'

    # Create Producer instance
    producer = Producer(config)

    try:
        response=requests.get(
            url=url+"companies",
            headers={
                "Authorization": 'Basic ZTA1YjQzYmYtNDY4OC00YWFjLWIxZWItOTY2MWUxYzZjMDA0Og==',
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate"
            },
            stream=True
        )

        print("Established connection to Company House UK streaming API")

        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')

                json_line = json.loads(decoded_line)

                # Build empty JSON
                company_profile = {
                    "company_name": None,
                    "company_number": None,
                    "company_status": None,
                    "date_of_creation": None,
                    "postal_code": None,
                    "published_at": None
                }

                try:
                    company_profile['company_name'] = json_line["data"]["company_name"]
                except KeyError:
                    company_profile['company_name'] = "NA"

                try:
                    company_profile['company_number'] = json_line["data"]["company_number"]
                except KeyError:
                    company_profile['company_number'] = "NA"
                
                try:
                    company_profile['company_status'] = json_line["data"]["company_status"]
                except KeyError:
                    company_profile['company_status'] = "NA"

                try:
                    company_profile["date_of_creation"] = json_line["data"]["date_of_creation"]
                except KeyError:
                    company_profile["date_of_creation"] = "NA"

                try:
                    company_profile["postal_code"] = json_line["data"]["registered_office_address"]["postal_code"]
                except KeyError:
                    company_profile["postal_code"] = "NA"

                try:
                    company_profile["published_at"] = json_line["event"]["published_at"]
                except KeyError:
                    company_profile["published_at"] = "NA"

                # Produce data to Kafka topics
                topics = {
                    "company_house": company_profile,
                }

                # Optional per-message delivery callback
                def delivery_callback(err, msg):
                    if err:
                        print('ERROR: Message failed delivery: {}'.format(err))
                    else:
                        print("Produced event to topic {}: key = {} value = {}".format(
                            msg.topic(), msg.key(), msg.value()))

                # Produce data to Kafka topics
                for topic, message in topics.items():
                    producer.produce(topic, key=message["company_number"].encode('utf-8'), value=json.dumps(message).encode('utf-8'), callback=delivery_callback)

                # Block until the messages are sent.

    except Exception as e:
        print(f"an error occurred {e}")

