DESCRIPTION:
This folder contains all files used to deploy and run the web application on AWS Elastic Beanstalk. These files define the Flask-based website for the SFU Parking project.


FOLDER STRUCTURE:
Elastic Beanstalk/
│
├── application.py
├── database_connection.py
├── Procfile
├── requirements.txt
│
├── static/
│   ├── app.js
│   └── styles.css
│
└── templates/
    ├── base.html
    ├── campus.html
    ├── index.html
    └── log_event.html


CONTENTS:
· application.py - the main flask application that defines all webpage routes, handles user requests, retrieves data from the database, and renders HTML templates (central controller for the website).
· database_connection.py - a helper that manages the PostgreSQL connection. It allows the web application to query occupancy tables, retrieve metrics, and display results on the website.
· Procfile - configuration file used by AWS Elastic Beanstalk to start the application.
· requirements.txt - lists all Python dependencies that must be installed during deployment (used by AWS Elastic Beanstalk).

· static/app.js - client-side logic for the interactive components of the website. It is responsible for rendering the donut chart representing real-time parking occupancy; drawing the historical occupancy chart using dynamic SVG paths; displaying recent events in a table; handling lot selection dropdown logic; fetching data from Flask endpoints; updating UI components; removing/replacing UI elements when data is missing.
· static/styles.css - defines the full visual style of the website, including layout, colors, typography, spacing, and component appearance.

· templates/base.html - the main layout for all pages in the website.
· templates/campus.html - the main dashboard page for each SFU campus. It displays the lot picker dropdown, shows the full dashboard interface, passes campus colors and identifiers, and connects directly to app.js for all rendering.
· templates/index.html - home page of the application shown when users land on the website, simple welcome screen.
· templates/log_event.html - template for the event logging page, empty. The file does not contain any details for now since we use data generator.


HOW IT WORKS:
The front-end files (/templates and /static scripts) communicate with Flask through /api/... endpoints to detch live data.