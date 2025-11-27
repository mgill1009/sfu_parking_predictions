DESCRIPTION:
This folder contains two Python scripts responsible for generating synthetic parking activity data. One script produces live real-time data. The other script generates multi-year. Historical data used to populate the data lake and train forecasting models.


FOLDER STRUCTURE:
Local Python Scripts/
│
├── generate_real_time_data.py
└── generate_historical_parking_data.py


CONTENTS:
· generate_real_time_data.py - runs a continuous real-time parking simulator; creates realistic ARRIVAL/DEPARTURE events based on class schedules; assigns lot dynamically; sends events to API Gateway; simulates peaks, weekend low demand and random variations.
· generate_historical_parking_data.py - generates historical parking logs spanning multiple semesters (2022-2025); uses full semester definitions, holidays, exam periods, special event days and realistic student class patterns; produces ARRIVAL/DEPARTURE events for tens of thousands of students; simulates lot choice based on capacity constraints; outputs CSV files containing cleaned, chronological parking logs


HOW IT WORKS:
- generate_real_time_data.py:
Generates a daily schedule -> assigns students to Burnaby/Surrey lots using real-time capacity checks -> creates realistic events with timestamps, session IDs, and license plates -> sends events in small batches to the API Gateway. Runs continuously
- generate_historical_parking_data.py:
Builds 3-year class schedules for thousands of students across Burnaby/Surrrey -> for each semester simulates busy weeks, regular weeks, and weekends -> converts schedules into arrival/departure events -> simulates real-time parking -> saves each semester's events as CSV files