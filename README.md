# tinybird_taxi_mvp

## How to Run

1. **Execute the Script:**
   - Run the main script with `python3 main.py`.

2. **Provide a Parquet URL (Optional):**
   - Optionally, input a valid Parquet URL from the [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page.
   - Right-click on the hyperlink of a Parquet file, copy the link, and paste it into the terminal when prompted.

3. **Select Year/Month (Optional):**
   - You can choose a specific year and month through the terminal for processing.

4. **Output Results to CSV (Optional):**
   - When prompted, input 'y' or 'n' to decide if you want to save the results locally as a CSV file.

## Thought Process

I began by reading the Parquet files using `pyarrow` and `pandas`. During my exploratory data analysis (EDA), I ensured that all files could be loaded correctly. I encountered issues with some timestamp columns not being read correctly, so I opted to stream them as strings to avoid errors. Additionally, I noticed that the "distance" columns had different names in older datasets, so I accounted for this variation.

To enhance the user experience, I initially considered storing the indexes of all results in a JSON file for faster retrieval. However, performance testing showed minimal time savings, so I decided to keep the project simple. The MVP allows users to either input a Parquet file URL for processing or select a year and month.

To accommodate different user needs, the script provides metrics on the total number of rows in the raw data and those in the 90th percentile, giving context to the results. Users can then choose to save the 90th percentile rows to a CSV file, which I selected for its broad usability.
