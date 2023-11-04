
# Investment Portfolio Tracker 📊
## Overview
I'm thrilled to introduce my latest project, an investment portfolio tracking system that has significantly improved my financial management strategies. This system efficiently tracks my investment portfolio's growth, enabling data-driven decision-making for my financial goals.

## Project Details
To ensure accessibility and affordability, this project makes use of various free resources:

Data Storage: Google Sheets for secure and reliable data storage.

ETL Process: Airflow for seamless data extraction, transformation, and loading.

Reporting: Matplotlib library for generating comprehensive and insightful reports.

Communication: Configured personal Gmail for SMTP within Airflow for effective communication.

Workflow
This system triggers daily updates once the market closes, facilitating a comprehensive monitoring process for both US and IND stock portfolios with just a single click.

## The workflow follows a structured Airflow Directed Acyclic Graph (DAG) process:

Extract: Pulls data from Google Sheets and transforms it into a structured DataFrame, including transaction details, stock, and mutual fund buying and selling rates.
Transform: Groups transaction data, calculates quantities and invested amounts, and employs SCD2 (Slowly Changing Dimension) to manage historical records. Additionally, it fetches real-time market data from the Google Finance API for up-to-date insights.
Load: Transformed data is loaded into a separate Finance tracker sheet.
Report: Automatic generation of daily reports using data from the Finance tracker.
Send_mail: A final DAG component that ensures timely reports are sent to respective emails.

## Future Prospects
This project's daily tracking not only enhances current financial decision-making but also lays the groundwork for the integration of machine learning for future predictions.

## Stay Updated
I will be continuously updating this repository with new features and insights. Feel free to reach out if you have any questions or if you'd like to contribute!

# Follow additional setup instructions in the README or documentation files
Acknowledgements
I extend my gratitude to the open-source community for their invaluable contributions and support.
