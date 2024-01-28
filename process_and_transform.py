import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

#Please add your subscription_id
subscription_id=<Subscription_id>
dict = {}

def aggregation(datetime,status,passenger_count):
    hour = datetime[0:13]
    pickup, dropoff = (0,1) if status == 'dropoff' else (1,0)

    if hour in dict.keys():
        dict[hour] = tuple(map(sum, zip(dict[hour], (pickup,dropoff,passenger_count))))
    else:
        dict[hour] = (pickup,dropoff,passenger_count)

    return dict


pipeline_options = PipelineOptions(streaming=True)
pipeline = beam.Pipeline(options=pipeline_options)

messages = (pipeline
    | beam.io.ReadFromPubSub(subscription=subscription_id).with_output_types(bytes)
    | beam.Map(json.loads)
    | beam.Filter(lambda x: x['ride_status'] != 'enroute')
    | beam.Map(lambda x: (x['timestamp'],aggregation(x['timestamp'],x['ride_status'],x['passenger_count'])))

    )

lines = messages | "print" >> beam.Map(print)

result = pipeline.run()
result.wait_until_finish()

