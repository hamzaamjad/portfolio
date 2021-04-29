# Portfolio
A portfolio of my work, includes descriptions of both public and private work.

# Python

## Public Projects

Below are Python scripts which are publically available in this Github for reference. Currently, the repository is not configured to be cloned and run -- the code described and hosted in this repository are primarily for reference purposes, highlighted as part of my public portfolio for potential employers and clients.

### Utility Scripts

The utility scripts provided below are a collection of helper code which is used in my Python scripts, such as the Bureau of Labor Statistics and FRED scripts described below. If I found myself using functionality frequently, and in order to facilitate a cleaner, more reusable codebase, I broke out code into an appopriate helper module.

- ```ppy_api.py```: contains Python objects created to interact with specific APIs. The functions within the objects provide users utilities to easily generate data identifiers and codes which can then be used to query and retrieve data from their respective API. Currently, contains two objects: 
    - ```FRED()```, which provides functions to interact with the Federal Reserve Bank of St. Louis public API.
    - ```BLS()```, which provides functions to interact with the Bureau of Labor Statistics public API

- ```ppy_auth.py```: an interface to AWS Secrets Manager, allowing retrieval of secrets hosted there, such as API keys or database credentials. This code was taken from AWS's self-help guidance on interacting with AWS Secrets Manager programatically, with minimal changes. This script requires that the AWS CLI is configured, and that the user has appropriate permissions to retrieve credentials from AWS Secrets Manager.

- ```ppy_box.py```: an interface to the Box API. Box is a cloud content management and file sharing service. The helper functions in this script allow users to navigate file structures within Box programatically, and perform basic operations on files. Functionality includes the ability to create new files, read and/or update existing files, and to delete files, providing basic CRUD functionality. The utilities in this script are used within the BLS and FRED scripts documented below, to upload a copy of the data as a flat file, prior to processing to a SQL database. This script requires a JSON token hosted on a users local machine in order to authenticate and interact with the Box API.

- ```ppy_geography.py```: contains functions to assist in the geocoding and identification of census geographic areas for a given address. Geocoding is taking a text-based address and returning the geographic coordinates, typically a latitude/longitude pair. Census geographic areas refer to various layers of geography for which the U.S. Census Bureau is responsible for delineating and identifying. The U.S. Government produces various datasets through entities such as the Census Bureau and Bureau of Labor Statistics which can be associated with these geographic areas. Therefore, it is useful to retrieve this information for a given address, as we can then use it to enrich our demographic understanding of a geographic location. For more information on Census Bureau geographic areas please visit: [About Geograhpic Areas](https://www.census.gov/programs-surveys/geography/guidance/geo-areas.html). This script references two external APIs in order to perform geocoding and retrieve information on Census geographic areas. The first is the [Google Maps Geocoding API](https://developers.google.com/maps/documentation/geocoding/start), which is used to perform geocoding on an address. Once an address is geocoded, the latitude/longitude pair is sent to the second API, the [Census Geocoder API](https://geocoding.geo.census.gov/), to retrieve geographic areas such as Metropolitan Statistical Area, Census Tract, and Census Block among other geographic information. These steps, and their associated functions, are unified under a single function ```address_geographies```, providing users an easy interface to retrieve an address's geographic coordinates and areas. This script requries a Google Maps API key enabled for Geocoding.

- ```ppy_sql.py```: functions which use the ```psycopg2``` library to interact with a PostgreSQL database. Helper functions contained within provide Python wrappers to common database functionality such as: connect, disconnect, execute & commit from user provided SQL, execute & fetchall rows from user provided SQL, copy from a file to a database table, drop a table, and return a list of tables within the active connection. This script is leveraged in the below BLS & FRED Python scripts to create the tables within the SQL database using ```execute_commit```, and bulk loading of data using the ```copy_from``` functionality to store data from flat files in the database.

- ```ppy_web.py```: a small script consisting of
    - a custom HTTP Adapter for the ```requests``` library, adding timeout functionality to web requests
    - a ```create_session``` function, which uses a custom Retry object from `urllib3` and the above HTTP Adapter to return a `requests.Session` object which has timeouts and retries specified based on user input to the function

- ```web.py```: a work-in-progress script, the long-term successor to the utilities in `ppy_web`. Contains cleaner code implementing a custom `requests.Session` object, which also includes a hook to assert that the response has returned a successfully (200), raising an error otherwise. Additionally, contains a custom `AsyncClient` object, which uses asynchronous libraries including asyncio, aiohttp and aiofiles to perform asynchronous web requests. Functions within this object enable a user to retreive web requests, pass a custom parser function for data extraction, and download files all within an asynchronous fashion. This enables faster web requests, typically used in creating web scrapers. Without asynchronous requests, users would have to wait for each request to resolve before continuing on to the next, which can significanlty slow down a script. From my own testing, using asynchronous processes when performing web scraping or other web data extraction projects on a larger number of links or data can speed up extraction by 5-20x. 

### Bureau of Labor Statistics

`bls.py` contains the logic which I use to extract, transform and load data from the Bureau of Labor Statistics into a SQL database for analysis. 

### FRED

### CME DataMine

DataMine is CME Group's historical data service, providing access to more than 450 terabytes of historical data. I actively maintained and improved the DataMine Python package, effectively a Python wrapper for the DataMine API. The Python package allows users to:

1. Load their data catalog, based on active subscriptions
2. Download data from catalog to their local machine
3. For select datasets, provide tools to automatically structure and load the retrieved files into a Pandas DataFrame, including correct data typing and transformations necessary to provide a ready for analysis DataFrame. 

More information on CME DataMine Python interface can be found at the following: [DataMine Python](https://github.com/CMEGroup/datamine_python)

## Private Projects

### Social Media Data Extraction

I was contracted by a client to extract data from various social media sites, and perform analysis on the data extracted to replace existing, manual Excel-based processes. I redesigned the analytics process for social media metrics to remove manual steps, and provide a more unified analytics pipeline. I acquired social media handles of concern, structured them into a centralized table, and used this single source of social media handles to provide a more structured process to systematically acquire information for parties of concern. I wrote Python scripts to extract data from YouTube, Instagram and Twitch. Data from these platforms is then transfomed and strucutred into files that enable analysis within a Tableau dashboard. I designed the Tableau dashboard collaboratively with the client. I began by mocking up the design of the dashboard as a wireframe, drawing from existing analytical workbooks and materials provided by client. Upon client signoff, I then created the dashboard within Tableau, driven off of the data extracted from the Python-based data pipelines. Code and references to the Tableau analysis for this project is not available due to the private nature of the contract, however the wireframe design which was presented and approved by the client is displayed below. I am actively maintaining this project for the client, and we plan on pursuing a second phase to expand the functionality to include additional social media platforms and extend the analysis produced within Tableau.

![Social Media Dashboard Wireframe](/misc/social_media_dashboard_wireframe.png?raw=true "Social Media Dashboard Wireframe")

# Tableau

## Demographic Visualization

Analyzed data extracted from the Bureau of Labor Statistics, FRED, and public data on COVID-19 hosted on Github. Data was extracted using the Python scripts described above. Data was joined and structured for analysis using Tableau Prep Builder. The workbook provides a high-level view of key employment and confidence metrics for the U.S. economy in the first two dashboards, the U.S. demographic snapshot and the U.S. financial snapshot. A more granular view of the data on a state-by-state basis is provided in the state demographic snapshot. The state demographic snapshot provides subsets of the data to users, which are easily accessible using a filter in the top right corner. Available subsets are Top 10 states by: size of labor force, month-over-month percentage change in unemployment, and the overall unemployment rate. Unfortunately, interactivity with descriptive tooltips and the dashboard itself is not available, as the dashboard is not hosted on a Tableau Server. A copy of the analysis in PDF format, beyond the screenshots pictured below, can be found in this repository using the following link: [Demographic Analysis](/tableau/demographic_analysis/demographic_analysis.pdf).

![U.S. Demographic Snapshot](/tableau/demographic_analysis/us_demographic_snapshot.png?raw=true "U.S. Demographic Snapshot")

![U.S. Financial Snapshot](/tableau/demographic_analysis/us_financial_snapshot.png?raw=true "U.S. Financial Snapshot")

![State Demographic Snapshot](/tableau/demographic_analysis/state_demographic_snapshot.png?raw=true "State Demographic Snapshot")

## Loan Portfolio Analysis

Using a hypothetical loan portfolio, created a set of Tableau dashboards to help analyze and understand the portfolio, providing tools to easily cross-filter / slice & dice the portfolio. Cross-filtering produces various subsets of the loan portfolio, for which the view and associated metrics are recalculated, including metrics such as weighted average loan-to-value, debt service coverage ratio, and debt yield. The portfolio overview dashboard enables a user to cross-filter and analyze the portfolio by: portfolio snapshot date, geographic region, delinquency, origination date, maturity date, and property type. The delinquency by state dashboard allows users to drill down to the geographic details of individual loans, stratifying loans, both peforming and non-performing, by origination year and geography. Loan data is enhanced with demographic data from the Bureau of Labor Statistics, providing a demographic overlay on top of the loan portfolio, as displayed within the delinquency view. Users can export cross-filtered views directly to PDF, or retrieve the data itself in Excel format for additional analysis. All dashboards within the workbook include descriptive tooltips. Unfortunately, interactivity with descriptive tooltips and the dashboard itself is not available, as the dashboard is not hosted on a Tableau Server. A copy of the analysis in PDF format, beyond the screenshots pictured below, can be found in this repository using the following link: [Loan Portfolio Analysis](/tableau/loan_portfolio_analytics/loan_portfolio_analysis.pdf).

![Portfolio Overview](/tableau/loan_portfolio_analytics/portfolio_overview.png?raw=true "Portfolio Overview")

![Delinquency by State](/tableau/loan_portfolio_analytics/delinquency_dashboard.png?raw=true "Delinquency by State")

## NYC Real Estate Market Snapshot

As part of my research on the state of the real estate market within New York City during the COVID-19 era, I compiled data from StreetEasy, REBNY and CBRE. Trends show that the NYC market has been significantly impacted by the COVID-19 crisis, with no clear answer on the timing of an economic recovery. However, if NYC does rebound back to pre-pandemic figures, it creates a significant opportunity for investors in the NYC market to realize rebound upside. A copy of the analysis in PDF format, beyond the screenshots pictured below, can be found in this repository using the following link: [NYC Real Estate Market](/tableau/nyc_real_estate_market/nyc_real_estate_market.pdf).

![StreetEasy](/tableau/nyc_real_estate_market/streeteasy.png?raw=true "StreetEasy")

![REBNY](/tableau/nyc_real_estate_market/rebny.png?raw=true "REBNY")

![CBRE](/tableau/nyc_real_estate_market/cbre.png?raw=true "CBRE")

## U.S. Bank FFIEC Call Reports

Created several views to understand how bank loans and loan losses had trended during the previous financial crisis, in order to inform how they may trend in the future during the resolution of the COVID-19 crisis and the distress it has created in a variety of markets, especially real estate. Data was sourced from an aggregator of the FFIEC Call Reports, quarterly filings by banks in which they report their loan book data. A copy of the analysis in PDF format, beyond the screenshots pictured below, can be found in this repository using the following link: [Bank Analysis](/tableau/bank_analysis/bank_analysis.pdf).

![Loans and Loan Losses](/tableau/bank_analysis/loans_loan_losses.png?raw=true "Loans and Loan Losses")

![All Banks Histogram](/tableau/bank_analysis/all_banks_histogram.png?raw=true "All Banks Histogram")

![Crisis NPL Snapshot](/tableau/bank_analysis/crisis_npl_snapshot.png?raw=true "Crisis NPL Snapshot")

# JavaScript / D3.js

## Tiny Charts

Tiny Charts is a collection of JavaScript functions, hosted within an [Observable](https://observablehq.com/) Notebook, which allow users to create sparklines and sparkline-like graphics. Graphics can be used inline, in tables or with traditional charts. The library currently includes the following graphic types: line, bar, area, histogram, tick plot, range. The project is under active development, I plan to expand the library with additional graphics to allow the creation of rich text narratives and data driven stories, with a focus on inline presentation of data. The project can be viewed at the following: [Tiny Charts](https://observablehq.com/@hamzaamjad/tiny-charts).

Some examples of Tiny Charts in action, beyond the examples displayed in the original notebook, can be found at the following:

- [COVID-19 - Cases by State](https://observablehq.com/@hamzaamjad/covid-cases-by-state): I forked an existing notebook from another user on the Observable platform, replacing their visualizations with graphics from the Tiny Charts library. Additionally, I added some visualizations from the library to extend their analysis. The data is pulled from [The COVID Tracking Project](https://covidtracking.com/) using an API call within the notebook.

- [Trending Job Markets](https://observablehq.com/@justinferrara/trending-job-markets): A visualization authored by another Observable user, focused on analyzing job market trends within U.S. metropolitan areas. Data was sourced by the user from the Bureau of Labor Statistics and the U.S. Census Bureau.

- [Demographic Update - Unemployment](https://observablehq.com/@hamzaamjad/demographic-update-unemployment): A short, descriptive analysis on high level unemployment trends across U.S. states. Highlights use of Tiny Charts graphics inline and in tables, demonstrating the impact that small visualizations can make within a compact space. The data is sourced from the Bureau of Labor Statistics by querying their public API, using the Python script described above. I ran SQL queries against this data to extract relevant subsets, such as the five largest decreases / increases in unemployment, to limit the data manipulation that needed to occur using JavaScript within the notebook.

- [One Hundred Collatz Conjectures](https://observablehq.com/@hamzaamjad/one-hundred-collatz-conjectures): An artistically-focused visualization, focuses on visualizing the hailstone sequence a number follows as it works through the conjecture. I found the description of the hailstone numbers, how they have multiple descents and ascents like hailstones in a cloud, fascinating and wished to visualize it. More information on the conjecture can be found on the following Wikipedia page: [Collatz conjecture](https://en.wikipedia.org/wiki/Collatz_conjecture)