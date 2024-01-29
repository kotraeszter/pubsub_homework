# Data Engineering Trial Task
This Python script processes the data stream from a public Cloud Pub/Sub, the taxirides-realtime, then it filters out some ride statuses and count the "dropoff"-s, "pickup"-s and the total number of passangers.

### Prerequisites
- Python 3.x

### Isntall Required Packages
```pip install -r requirements.txt```

### Set up the credentials for service accounts
```export GOOGLE_APPLICATION_CREDENTIALS='\path\key.json'```

### Usage
- Please add you subscription_id to the process_and_transform.py before you start running it.
