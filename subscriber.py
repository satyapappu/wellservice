from google.cloud import pubsub
from pprint import pprint
from inspect import getmembers

def subscribe():
    pubsub_client_publisher = pubsub.Client("data-managers-search")
    topic = pubsub_client_publisher.topic("sp-wells-topic")
    pubsub_client_receiver = pubsub.Client("cds-dev-155819")

    subscriptions = pubsub_client_receiver.list_subscriptions()
    for sub1 in subscriptions:
        print(sub1.full_name)

    subscription = pubsub.subscription.Subscription("sp-test-subscription", topic)
    if not subscription.exists():
        subscription.create(pubsub_client_receiver)

    print('Subscription {} created on topic {}.'.format(
        subscription.full_name, topic.full_name))

    while True:
        pulled = subscription.pull()

        print 'Received messages'

        for ack_id, message in pulled:
            try:
                print 'Received message {} ack_id {}'.format(message, ack_id)
            except Exception as e:
                print 'Error {}'.format(e.message)
            else:
                subscription.acknowledge([ack_id])
