import logging
import json
from google.cloud import datastore
from google.cloud import pubsub
from flask import Flask, request, jsonify, make_response, render_template
import main

def publish(uwi, topicName):
    logging.info('Publishing message {} to topic {}.'.format(id, topicName))
    #print('Publishing message {} to topic {}.'.format(id, topicName))

    ds = datastore.Client(project="data-managers-search")
    kind = "SPWells"

    query = ds.query(kind=kind)
    query.order = ['uwi']
    result = main.get_fetch_results(query, uwi)
    if len(result) == 0:
        return make_response("Well not found", 404)

    entity = result[0]
    geolocation = {}
    geolocation['latitude'] = entity['latitude']
    geolocation['longitude'] = entity['longitude']

    wellObj = {}
    wellObj['uwi'] = entity['uwi']
    wellObj['name'] = entity['name']
    wellObj['md'] = entity['md']
    wellObj['spuddate'] = entity['spuddate']
    wellObj['location'] = geolocation
    
    topic_project_name = topicName.split("/")[1]
    pubsub_client = pubsub.Client(topic_project_name)

    topicName = topicName.split("/")[3]
    topic = pubsub_client.topic(topicName)

    #print 'Topic {}.'.format(topic.full_name)
    #print('Postin {}.'.format(json.dumps(wellObj)))

    message_id = topic.publish(json.dumps(wellObj))

    #print "done sending message {}".format(message_id)

    return make_response(jsonify({"messageId": long(message_id)}))

