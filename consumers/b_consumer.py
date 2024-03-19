import pandas as pd
from kafka import KafkaConsumer
import json
from db import dbconnect, DatabaseError, BROKER
import sys

if __name__ == "__main__":
    consumer = KafkaConsumer('data-breakfast', bootstrap_servers=BROKER)
    data = None
    engine, session = dbconnect()
    print("Listen.....", flush=True)
    for message in consumer:
        data = message.value.decode('utf-8')
        k = json.loads(data)
        df = pd.read_json(k)
        print(df, flush=True)
        try:
            df.to_sql(name='weather', con=engine,
                      if_exists='append', index=False)
            print("Insert Data", flush=True)
        except DatabaseError as e:
            print(e, file=sys.stderr, flush=True)
