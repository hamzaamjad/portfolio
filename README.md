# Portfolio
A portfolio of my work, includes descriptions of both public and private work.

# Python

## Public Projects

### Utility Scripts

### Bureau of Labor Statistics

### FRED

### CME DataMine

DataMine is CME Group's historical data service, providing access to more than 450 terabytes of historical data. I actively maintained and improved the DataMine Python package, effectively a Python wrapper for the DataMine API. The Python package allows users to:

1. Load their data catalog, based on active subscriptions
2. Download data from catalog to their local machine
3. For select datasets, provide tools to automatically structure and load the retrieved files into a Pandas DataFrame, including correct data typing and transformations necessary to provide a ready for analysis DataFrame. 

More information on CME DataMine Python interface can be found at the following: [DataMine Python](https://github.com/CMEGroup/datamine_python)

## Private Projects

### Social Media Data Extraction

I was contracted by a client to extract data from various social media sites, and perform analysis on the data extracted to replace existing, manual Excel-based processes. I redesigned the analytics process for social media metrics to remove manual steps, and provide a more unified analytics pipeline. I acquired social media handles of concern, structured them into a centralized table, and used this single source of social media handles to provide a more structured process to systematically acquire information for parties of concern. I wrote Python scripts to extract data from YouTube, Instagram and Twitch. Data from these platforms is then transfomed and strucutred into files that enable analysis within a Tableau dashboard. I designed the Tableau dashboard collaboratively with the client. I began by mocking up the design of the dashboard as a wireframe, drawing from existing analytical workbooks and materials provided by client. Upon client signoff, I then created the dashboard within Tableau, driven off of the data extracted from the Python-based data pipelines. Code and references to the Tableau analysis for this project is not available due to the private nature of the contract. I am actively maintaining this project for the client, and we plan on pursuing a second phase to expand the functionality to include additional social media platforms and extend the analysis produced within Tableau.

# Tableau

## Demographic Analysis

## Loan Portfolio Analysis

Using a hypothetical loan portfolio, created a set of Tableau dashboards to help analyze and understand a loan portfolio, providing tools to easily cross-filter / slice & dice the portfolio. Cross-filtering produces various subsets of the loan portfolio, for which the view and associated metrics are recalculated, including metrics such as weighted average loan-to-value, debt service coverage ratio, and debt yield. Loan data is enhanced with demographic data from the Bureau of Labor Statistics, providing a demographic overlay on top of the loan portfolio. The workbook enables a user to cross-filter and analyze the portfolio by: portfolio snapshot date, geographic region, delinquency, origination date, maturity date and property type. Users can export cross-filtered views directly to PDF, or retrieve the data itself in Excel format for any additional analysis. A copy of the analysis beyond the screenshot pictured below can be found in this repository, using the following link: placeholder.

## U.S. Bank FFIEC Call Report Analysis

# JavaScript / D3.js

## Tiny Charts

Tiny Charts is a collection of JavaScript functions, hosted within an [Observable](https://observablehq.com/) Notebook, which allow users to create sparklines and sparkline-like graphics. Graphics can be used inline, in tablesor with traditional charts. The library currently includes the following graphic types: line, bar, area, histogram, tick plot, range. The project is under active development, I plan to expand the library with additional graphics to allow the creation of rich text narratives and data driven stories, with a focus on inline presentation of data. The project can be viewed at the following: [Tiny Charts](https://observablehq.com/@hamzaamjad/tiny-charts).

Some examples of Tiny Charts in action, beyond the examples displayed in the original notebook, can be found at the following:

- [COVID-19 - Cases by State](https://observablehq.com/@hamzaamjad/covid-cases-by-state): I forked an existing notebook from another user on the Observable platform, replacing their visualizations with graphics from the Tiny Charts library. Additionally, I added some visualizations from the library to extend their analysis. The data is pulled from [The COVID Tracking Project](https://covidtracking.com/) using an API call within the notebook.

- [Trending Job Markets](https://observablehq.com/@justinferrara/trending-job-markets): A visualization authored by another Observable user, focused on analyzing job market trends within U.S. metropolitan areas. Data was sourced by the user from the Bureau of Labor Statistics and the U.S. Census Bureau.

- [Demographic Update - Unemployment](https://observablehq.com/@hamzaamjad/demographic-update-unemployment): A short, descriptive analysis on high level unemployment trends across U.S. states. Highlights use of Tiny Charts graphics inline and in tables, demonstrating the impact that small visualizations can make within a compact space. The data is sourced from the Bureau of Labor Statistics by querying their public API, using the Python script described above. I ran SQL queries against this data to extract relevant subsets, such as the five largest decreases / increases in unemployment, to limit the data manipulation that needed to occur using JavaScript within the notebook.

- [One Hundred Collatz Conjectures](https://observablehq.com/@hamzaamjad/one-hundred-collatz-conjectures): An artistically-focused visualization, focuses on visualizing the hailstone sequence a number follows as it works through the conjecture. I found the description of the hailstone numbers, how they have multiple descents and ascents like hailstones in a cloud, fascinating and wished to visualize it. More information on the conjecture can be found on the following Wikipedia page: [Collatz conjecture](https://en.wikipedia.org/wiki/Collatz_conjecture)