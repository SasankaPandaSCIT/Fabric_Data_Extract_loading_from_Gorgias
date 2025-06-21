# Monthly Dashboard Data Ingestion

This notebook automates the process of fetching data from the Gorgias API, processing it, and storing it in a Delta table for a monthly dashboard. It also handles updates to existing records and new data insertions.

## Overview

The notebook performs the following key steps:

1.  **Environment Setup**: Installs necessary Python libraries.
2.  **Authentication**: Loads service account credentials for Google Sheets API access.
3.  **API Utility Functions**: Defines helper functions for API calls with retry mechanisms and date calculations.
4.  **Data Extraction from Google Sheets**: Reads client and authentication details from a specified Google Sheet.
5.  **Gorgias API Data Processing**: Iterates through each client, fetches user data and various metrics (First Response Time, One-Touch Tickets, Closed Tickets, Messages Sent, Customer Satisfaction, Average Resolution Time) from the Gorgias API for the past 19 months.
6.  **Data Transformation**: Collects and transforms the fetched data into a Pandas DataFrame, then converts it to a Spark DataFrame.
7.  **Timestamping**: Adds `Created_Datetime` and `Last_updated_time` columns to the processed data.
8.  **Delta Table Management**:
      * Reads existing data from the `Monthly_Dashboard` Delta table.
      * Compares the newly fetched data with existing records.
      * Updates `Created_Datetime` for matching records to preserve the original creation timestamp.
      * Merges the new and existing data, effectively updating existing records and inserting new ones.
      * Overwrites the `Monthly_Dashboard` Delta table with the merged data.

## Prerequisites

  * Azure Synapse Analytics or Microsoft Fabric workspace with a Spark enabled environment.
  * A Google Cloud Platform (GCP) service account with access to Google Sheets, stored as a JSON file named `service_account.json` in the notebook's `builtin` directory.
  * A Google Sheet containing client information with the following columns:
      * `Client`: Name of the client.
      * `Authentication`: Base64 encoded API authorization token for Gorgias.
      * `Channel`: The communication channel (e.g., 'email', 'chat').
  * A Delta table named `Monthly_Dashboard` in your Lakehouse (or a specified equivalent) to store the processed data. This table needs to be created manually once with the appropriate schema before the first run, or the notebook can create it on first run by uncommenting the relevant line.

## Setup and Installation

The first cell of the notebook installs the required libraries:

```python
%pip install google-auth google-auth-oauthlib google-auth-httplib2 gspread pandas
```

## Configuration

  * **`file_path`**: Ensure your `service_account.json` file is located at `./builtin/service_account.json`.

  * **`SHEET_ID`**: Update the `SHEET_ID` variable in the notebook to point to your specific Google Sheet:

    ```python
    SHEET_ID = "YOUR_GOOGLE_SHEET_ID_HERE"
    ```

    (e.g., `SHEET_ID = "101FEGEv6lu8mPcozqbimKzWRSH0e103ZApjTaRpHL2w"`)

  * **Delta Table Name**: The output Delta table is named `Monthly_Dashboard`. If you wish to use a different name, update the `saveAsTable` calls.

## Functions

  * `call_api_with_retries(api_func, *args, max_retries=5, base_wait=2)`: A generic function to call an API function with exponential backoff for handling 429 (Too Many Requests) errors.
  * `process_data_for_monthly_dashboard(filtered_df, channel, client, authorization)`: Iterates through filtered user data, calls various Gorgias API endpoints to fetch metrics for the past 19 months, and returns a list of dictionaries with the processed data.
  * `get_month_start_end(months_back: int = 1)`: Calculates the start and end datetime strings for a month a specified number of `months_back` from the current date, with a timezone offset of "-05:00".
  * `process_users(client, authorization, max_retries=5, initial_delay=5)`: Fetches user data from the Gorgias API for a given client and authorization, including retry logic. It filters users whose email contains 'getwrrk'.
  * `call_api_for_FRT(...)`, `call_api_for_closed_tickets(...)`, `call_api_for_satisfaction_survey(...)`, `call_api_for_messages_sent(...)`, `call_api_median_resolution(...)`, `call_api_for_one_touch(...)`: These functions are responsible for making specific API calls to Gorgias to retrieve the respective metrics. They include retry mechanisms to handle rate limiting.

## Usage

1.  **Upload `Dashboard v7.ipynb`** to your Synapse or Fabric workspace.
2.  **Upload `service_account.json`** to the `./builtin/` directory within your notebook's file system.
3.  **Update `SHEET_ID`** in the notebook's code cell 14 with your Google Sheet's ID.
4.  **Run All Cells**: Execute all cells in the notebook.

The notebook will:

  * Read client configurations from your Google Sheet.
  * Fetch relevant metrics from the Gorgias API for each client and user, spanning the last 19 months.
  * Structure the data into a Spark DataFrame.
  * Load existing data from `Data_from_Bigquery.monthly_dashboard`.
  * Perform a join to identify common records and retain the original `Created_Datetime` for these.
  * Merge the new and existing data, updating existing entries and adding new ones.
  * Overwrite the `Monthly_Dashboard` Delta table with the latest merged dataset.

## Troubleshooting

  * **Rate Limiting (429 Errors)**: The API call functions include retry mechanisms with exponential backoff to handle Gorgias API rate limits. If you encounter persistent 429 errors, consider increasing `max_retries` or `base_wait` in `call_api_with_retries`.
  * **Authentication Errors**: Double-check that your `service_account.json` is correctly formatted and has the necessary permissions to access Google Sheets. Also, ensure your Gorgias API authorization token in the Google Sheet is valid.
  * **`df_updated` not defined**: If you restart the kernel and run cells out of order, `df_updated` might not be defined. Rerun the cells sequentially from the beginning, especially the cell where `df_updated` is initialized (`df_updated = pd.DataFrame(...)`) and the subsequent loop that populates it.
  * **Table Not Found**: If the `Monthly_Dashboard` table does not exist on the first run, the line `df_original_dashboard = spark.sql("SELECT * FROM Data_from_Bigquery.monthly_dashboard")` will fail. Uncomment and run `df_original_dashboard.write.format("delta").save("Tables/Monthly_Dashboard_New1")` (or the `saveAsTable` version) once to create the initial table. You might need to adjust the path/table name based on your Lakehouse setup.
  * **Data Type Mismatches**: The notebook attempts to cast some values to integers (`int(api_result_FRT) if api_result_FRT else 0`). If the API returns unexpected non-numeric values for these fields, it might lead to errors. Ensure the API responses align with expected numeric types or add more robust error handling/type conversion.
