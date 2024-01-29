from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json

subscription_id = ''

subscriber = pubsub_v1.SubscriberClient()

duplicates = set()
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    ride_id = json.loads(message.data)['ride_id']
    if ride_id in duplicates:
        print('Van duplikáció')
        pass
    else:
        duplicates.add(ride_id)
        print(f"Received {message}.")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_id, callback=callback)
print(f"Listening for messages on {subscription_id}..\n")

with subscriber:
    while True:
        try:
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
