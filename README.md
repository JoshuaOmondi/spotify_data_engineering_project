# Spotify Data Engineering Project

![Project Workflow](images/project_flow_chart.png)

## Overview

This project demonstrates a basic data engineering workflow. The goal is to extract data from the Spotify API, load it into BigQuery for transformation, and visualize the results using Looker Studio.

## Project Workflow
1. Data Extraction:

Used Python and the Spotipy library to extract data from the Spotify API.
The data includes details about artists and their top tracks from various African countries.
2. Data Loading:

Loaded the extracted data into Google BigQuery.
Used BigQuery for further data transformations and analysis.
3. Data Visualization:

Connected BigQuery to Looker Studio for creating interactive dashboards and visualizing the data.
## Project Structure
scripts/: Contains Python scripts used for data extraction and loading.
images/: Contains images used in this README or related documentation.
queries/: Contains SQL queries used for data transformations in BigQuery.
dashboard/: Screenshots and details of the Looker Studio dashboard.
Getting Started
Prerequisites
Python 3.x
Spotipy library
Google Cloud SDK
BigQuery
Looker Studio
Setup Instructions
Clone the Repository:


Load Data into BigQuery:

Configure your Google Cloud credentials and run the data loading script.
Visualize Data in Looker Studio:

Connect your BigQuery dataset to Looker Studio and create your dashboard.
Results
Screenshots of the Looker Studio dashboard and sample queries can be found in the dashboard/ and queries/ directories.

# Future Work
Add more data sources to enrich the analysis.
Implement more complex transformations in BigQuery.
