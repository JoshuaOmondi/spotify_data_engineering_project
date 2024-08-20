# Spotify Data Engineering Project

## Overview

This project demonstrates a basic data engineering workflow. The goal is to extract data from the Spotify API, load it into BigQuery for transformation, and visualize the results using Looker Studio.

## Project Workflow

![Project Workflow](images/project_flow_chart.png)

1. **Data Extraction**:
   - Used Python and the Spotipy library to extract data from the Spotify API.
   - The data includes details about artists and their top tracks from various African countries.

2. **Data Loading**:
   - Loaded the extracted data into Google BigQuery.
   - Used BigQuery for further data transformations and analysis.

3. **Data Visualization**:
   - Connected BigQuery to Looker Studio for creating interactive dashboards and visualizing the data.

## Results

![Dashboard Screenshot](images/dashboard_screenshot.png)

## Getting Started

### Prerequisites

- Python 3.x
- Spotipy library
- Google Cloud SDK
- BigQuery
- Looker Studio

