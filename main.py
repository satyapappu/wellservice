import json
import logging
import time
from flask import Flask, request, jsonify, make_response, render_template
from flask_cors import CORS 
from google.cloud import datastore
import publisher
from google.cloud import storage, exceptions
from google.cloud.storage import Blob
from google.cloud import pubsub

app = Flask(__name__)

globalproject = "data-managers-search"
wellKind = "SPWells"

def __init__(self):
    self.ds = datastore.Client(project=globalproject)
    self.kind = wellKind

@app.route('/')
def parse_wells_json():
    with open('wells.json') as data_file:    
        data = json.load(data_file)
    mystring = "Start:"
    for well in data:
        mystring = mystring + "Wells,"
        for attribute, value in well.iteritems():
            mystring = mystring + attribute + ":" + str(value) + ", " 
    return mystring

@app.route('/api/sortedlist', methods=['GET'])
def sorted_unique_list():
    dsClient = datastore.Client(project=globalproject)
    kind = wellKind
    query = dsClient.query(kind=kind)
    query.distinct_on = ['uwi']
    query.order = ['uwi']

    allCountries = list()
    queryResults = query.fetch()
    for entity in queryResults:
        allCountries.append(dict(entity))

    return json.dumps(allCountries)

@app.route('/api/wells', methods=['PUT'])
def insert_well():
    name = request.get_json()['name']
    spuddate = request.get_json()['spuddate']
    md = request.get_json()['md']
    longitude = request.get_json()['location']['longitude']
    latitude = request.get_json()['location']['latitude']
    uwi = request.get_json()['uwi']
        
    dsClient = datastore.Client(project=globalproject)
    kind = wellKind
    entitykey = dsClient.key(kind, uwi)
    entity = datastore.Entity(key=entitykey)

    entity.update(
        {
            'name':name,
            'spuddate':spuddate,
            'uwi':uwi,
            'md':md,
            'latitude':latitude,
            'longitude':longitude
        }
    )

    logging.info(entitykey)
    logging.info(entity)
    dsClient.put(entity)
    
    return make_response('done')

@app.route('/api/wells', methods=['GET'])
def list_wells():
    
    queryparam = request.args.get('query', '')
    searchparam = request.args.get('search', '')
      
    dsClient = datastore.Client(project=globalproject)
    kind = wellKind

    query = dsClient.query(kind=kind)
    allWells = list()
    queryResults = list()

    # No parameters - all data
    if queryparam == "" and searchparam == "":
        queryResults = list(query.fetch())
    # Query parameter 
    elif queryparam != "":
        pos = queryparam.find(":")
        prop = queryparam[:pos]
        v = queryparam[pos+1:]
        if prop == "longitude" or prop == "latitude":
            val = float(v)
        else:
            val = v
        query.add_filter(prop, "=", val)
        queryResults = list(query.fetch())
    # Search parameter
    elif searchparam != "":
        # Fire 7 queries for each column and append the results
        results1 = list()
        query1 = dsClient.query(kind=kind)
        query1.add_filter("uwi", "=", searchparam)
        results1 = list(query1.fetch())

        results2 = list()
        query2 = dsClient.query(kind=kind)
        query2.add_filter("name", "=", searchparam)
        results2 = list(query2.fetch())

        results3 = list()
        query3 = dsClient.query(kind=kind)
        query3.add_filter("spuddate", "=", searchparam)
        results3 = list(query3.fetch())

        results4 = list()
        query4 = dsClient.query(kind=kind)
        query4.add_filter("md", "=", searchparam)
        results4 = list(query4.fetch())

        results6 = list()
        query6 = dsClient.query(kind=kind)
        query6.add_filter("latitude", "=", searchparam)
        results6 = list(query6.fetch())

        results7 = list()
        query7 = dsClient.query(kind=kind)
        query7.add_filter("longitude", "=", searchparam)
        results7 = list(query7.fetch())

        queryResults = results1 + results2 + results3 + results4 + results5 + results6 + results7

        # Get all records and filter on column values
        # for entity in list(query.fetch()):
        #     if str(entity["id"]) == searchparam or entity['name'] == searchparam or entity['country'] == searchparam or entity['countryCode'] == searchparam or entity['continent'] == searchparam or str(entity['latitude'])== searchparam or str(entity['longitude']) == searchparam:
        #         queryResults.append(dict(entity))
        
    # Final Formatting of data into JSON with Locations
    i = 1
    for entity in queryResults:
        allWells.append(dict(entity))
        i = i+1
        if i > 20:
            break

    data = []
    for entity in allWells:
        item = {}
        geoloc = {}
        geoloc['longitude'] = entity['longitude']
        geoloc['latitude'] = entity['latitude']
        item['md'] = entity['md']
        item['spuddate'] = entity['spuddate']
        item['name'] = entity['name']
        item['location'] = geoloc
        item['uwi'] = entity['uwi']
        data.append(item)

    jsonWells = json.dumps(data)
    return jsonWells
    
@app.route('/api/wells/<string:uwi>', methods=['GET'])
def fetch_well(uwi):
    ds = datastore.Client(project=globalproject)
    kind = wellKind

    query = ds.query(kind=kind)
    query.add_filter('uwi', "=", uwi)
    results = list()
    for entity in list(query.fetch()):
        results.append(dict(entity))
    
    if len(results) == 0:
        return make_response("Well not found", 404)

    entity = results[0]
    geolocation = {}
    geolocation['longitude'] = entity['longitude']
    geolocation['latitude'] = entity['latitude']

    wellObj = {}
    wellObj['md'] = entity['md']
    wellObj['uwi'] = entity['uwi']
    wellObj['name'] = entity['name']
    wellObj['location'] = geolocation
    wellObj['spuddate'] = entity['spuddate']
    
    return jsonify(wellObj)

@app.route('/api/wells/<string:uwi>/store', methods=['POST'])
def store_well(uwi):
    bucketName = request.get_json()['bucket']

    logging.info('Storing well {} to bucket {}.'.format(uwi, bucketName))
    
    # Fetch entity with id
    ds = datastore.Client(project=globalproject)
    kind = wellKind
    query = ds.query(kind=kind)
    query.order = ['uwi']
    result = get_fetch_results(query, uwi)
    if len(result) == 0:
        return make_response("Well not found", 404)
    entity = result[0]

    geolocation = {}
    geolocation['longitude'] = entity['longitude']
    geolocation['latitude'] = entity['latitude']

    wellObj = {}
    wellObj['md'] = entity['md']
    wellObj['uwi'] = entity['uwi']
    wellObj['name'] = entity['name']
    wellObj['location'] = geolocation
    wellObj['spuddate'] = entity['spuddate']
    
    jsonObj = json.dumps(wellObj)

    gcs = storage.Client(project=globalproject)

    try:
        # Check if the bucket exists
        bucket = gcs.get_bucket(bucketName)
        
        #store json to bucket
        filename = str(id)
        blob = Blob(filename, bucket)
        try:
            data = jsonObj.encode('utf-8')
            blob.upload_from_string(data, content_type='text/plain')
            logging.info("File " + filename + " stored in bucket " + bucketName)
            return make_response("Successfully stored in GCS", 200)
        except :
            return make_response('Error: Cannot store json object', 404)
    except exceptions.NotFound:
        return make_response('Error: Bucket {} does not exist.'.format(bucketName), 404)
    except exceptions.BadRequest:
        return make_response('Error: Invalid bucket name {}'.format(bucketName), 400)
    except exceptions.Forbidden:
        return make_response('Error: Forbidden, Access denied for bucket {}'.format(bucketName), 403)


@app.route('/api/wells/<string:uwi>', methods=['DELETE'])
def delete_well(uwi):
    client = datastore.Client(project=globalproject)
    kind = wellKind
        
    if isinstance(uwi, basestring) == False:
        logging.info("input not string")
        return make_response("Unexpected error", 500)

    #Fetch entity with id
    query = client.query(kind=kind)
    query.add_filter('uwi', "=", uwi)
    entities = list(query.fetch())
    if len(entities) > 0:
        entity = entities[0]
        deleteKey = entity.key
        
        logging.info(deleteKey)
        logging.info(entity)
        logging.info("Satya - Delete using key")   
        client.delete(deleteKey)

        time.sleep(1)   #1 second delay to allow delete to happen before the GET

        # try to get the deleted record to ensure it is deleted - try 3 times
        numtries = 0
        query = client.query(kind=kind)
        query.add_filter('uwi', "=", uwi)
        while numtries < 4:
            results = list()
            for entity in list(query.fetch()):
                results.append(dict(entity))
            if len(results) != 0:
                numtries = numtries + 1
            else:
                break

        return make_response("deleted", 200)
    else:
        return make_response("Well record not found", 404)

def get_fetch_results(query, uwi):
    results = list()
    for entity in list(query.fetch()):
        if entity["uwi"] == uwi:
            results.append(dict(entity))
    return results

@app.route('/api/wells/<string:uwi>/publish', methods=['POST'])
def publish_well(uwi):
    topic = request.get_json()['topic']
    return publisher.publish(uwi, topic)

@app.route('/api/getnotification', methods=['POST'])
def get_notification():
    name = request.get_json()['name']
    uwi = request.get_json()['uwi']
    return name + " " + uwi

@app.route('/api/pullnotification', methods=['GET'])
def pull_notification():
    pubsub_client_publisher = pubsub.Client("data-managers-search")
    topic = pubsub_client_publisher.topic("sp-wells-topic")
    pubsub_client_receiver = pubsub.Client("data-managers-search")
    subscription = pubsub.subscription.Subscription("sp-test-subscription", topic)

    if not subscription.exists():
        subscription.create(pubsub_client_receiver)

    print('Subscription {} created on topic {}.'.format(subscription.full_name, topic.full_name))
    
    pulled = subscription.pull(return_immediately=True)
    print 'Received {} messages'.format(len(pulled))

    for ack_id, message in pulled:
        try:
            print '* {}: {}, {}'.format(message.message_id, message.data, message.attributes)
        except Exception as e:
            print 'Error {}'.format(e.message)
        else:
            subscription.acknowledge([ack_id])
    
    return "done"

@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    #return 'An internal error occurred.', 500
    return make_response('Unexpected error', 500)

        
if __name__ == '__main__':
    # Used for running locally
    app.run(host='127.0.0.1', port=8081, debug=True)
