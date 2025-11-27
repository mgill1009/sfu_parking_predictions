DESCRIPTION:
This folder contains the sever less functions used in the project. These Lambda functions handle real-tim data ingestion, event stream processing, and updating the recent_events table in the DB. Together, they connect the real-time simulator output with the web application by maintaining up-to-date log of recent events.


FOLDER STRUCTURE:
AWS Lambda/
│
├── recent_events.py
├── psycopg2-layer.zip
├── lambda_ingester.py
└── lambda_kinesis_processor.py


CONTENTS:
· recent_events.py - maintains the recent_events PostgreSQL table used by the website. This lambda is responsible for inserting new arrival and departure events into the database; keeping only the latest 5 events to limit table size; formatting timestamps and normalizing structure. It is invoked by Kinesis processor after new events are ingested.
· lambda_ingester.py - processes raw parking events arriving into the system. It is responsible for validating the event payload structure; normalizing fields; writing the cleaned events; ensuring event stream remains consistent.
· lambda_kinesis_processor.py - triggered by Kinesis Data Stream batches. The function decodes incoming event records; extracts events in real time; updates current_lot_occupancy table in postgreSQL.
· psycopg2-layer.zip - python package that allows AWS Lambda to connect to postgreSQL RDS

HOW IT WORKS:
lambda_ingester.py validates incoming parking events and sends them into the Kinesis stream after being triggered by API Gateway.
lambda_kinesis_processor.py and recent_events.py are triggered automatically by the Kinesis stream.

