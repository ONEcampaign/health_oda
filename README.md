
# The troubling hidden trend in health aid

Surge financing for COVID-19 disguised a downward trend in health aid, which reached a 13-year low in 2021 and only rebounded slightly in 2022. These short-sighted funding decisions are putting global health gains at risk.


This repository contains scripts and tools for analysing Official Development Assistance (ODA) data related to health, focusing on trends in health aid, including and excluding COVID-19 related flows. 

It enables users to analyse bilateral and multilateral health ODA, group recipients by region and income levels, and compare health aid trends across donors, regions, and income groups.

## Project Overview

This analysis supports ONE's data story, "Surge financing for COVID-19 is disguising a downward trend in health aid," which explores the impact of COVID-19 on overall health aid flows. The analysis compares health ODA (including and excluding COVID-19 aid) from 2008 to 2022, revealing a 13-year low in health aid when excluding COVID-19 funding.

All financial data are presented in 2022 US dollars, adjusted for constant prices and exchange rates using OECD DAC deflators.

## Table of Contents

- [Project Overview](#project-overview)
- [Data Sources](#data-sources)
- [Methodology](#methodology)
- [Key files](#key-files)
- [Accessing data](#accessing-data)

## Data Sources

This analysis uses ODA data from:
- [OECD Creditor Reporting System (CRS)](https://stats.oecd.org/Index.aspx?DataSetCode=CRS1) health ODA to all developing countries.
- - ONE's own calculations for sectoral multilateral imputations using [Members' Total Use of the Multilateral System Database](https://stats.oecd.org), together with the CRS.


## Methodology

Health ODA is analyzed using purpose codes under the OECD DAC health sectors (DAC Codes 120-123), with specific exclusions for COVID-19 related flows based on:
- Projects flagged with COVID-19-related keywords
- DAC purpose code 12264 (COVID-19 control)
- Contributions to the COVID-19 Response and Recovery Multi-Partner Trust Fund

For a more detailed methodology, refer to ONE's [methodology note](https://observablehq.com/@one-campaign/hidden-trend-in-health-financing).

## Key files
- **`all_donors_all_recipients.py`**: Compares health ODA with and without COVID-19 for all donors for all developing countries (using CRS data).
- **`all_donors_recipient_groupings.py`**: Groups health ODA by recipient region and income level.
- **`bilateral.py`**: Extracts and processes bilateral health ODA from the OECD CRS.
- **`imputed_multilateral.py`**: Handles imputed multilateral aid calculations.
- **`common.py`**: Contains common helper functions for data processing, including filtering by purpose codes.


## Accessing data
The results are saved as CSV files in the `output` directory, with constant prices based on the year 2022.

Please reach out if you need additional or different outputs.

For more information on ONE's methodology for multilateral sector imputations, please see [this document](https://cdn.one.org/international/media/international/2021/05/04101117/Imputed-Multilateral-Sectors-Methodology.pdf).

All of ONE's research and analysis is open source. Code to reproduce this analysis can be found in this [GitHub repository](https://github.com/ONEcampaign/health_oda).

For more of ONE's ODA analysis:
- ONE's [ODA topic](https://data.one.org/topics/official-development-assistance/) page.
- ONE's [Aid Dashboard](data.one.org/aid-dashboard).

ONE develops several python packages and tools to work with OECD DAC data, including:
- [oda-reader](https://github.com/ONEcampaign/oda_reader): a python package to access OECD DAC data from the data-explorer API.
- [oda-data](https://github.com/ONEcampaign/oda_data_package): a python package to reproduce all of ONE's ODA analysis with a few lines of code
- [pydeflate](https://github.com/jm-rivera/pydeflate): a python package to convert flows to constant prices and exchange rates, using IMF, World Bank, or DAC data price deflators and exchange rates data.

