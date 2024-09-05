#!/bin/bash
# ETL script for CoreDataEngineers

# Set the environment variable for the CSV URL
CSV_URL=${CSV_URL}

# Step 1: Extract
echo "Starting the extraction process..."
mkdir -p raw
curl -o raw/annual-enterprise-survey.csv "$CSV_URL"

if [ -f raw/annual-enterprise-survey.csv ]; then
    echo "File successfully downloaded to the raw directory."
else
    echo "Failed to download the file."
    exit 1
fi

# Step 2: Transform
echo "Starting the transformation process..."
mkdir -p transformed

# Use awk to rename Variable_code and extract only specific columns
awk -F',' 'BEGIN { OFS="," }
NR==1 {
    # Save header positions and rename Variable_code to variable_code
    for(i=1; i<=NF; i++) {
        if($i == "year") year_col = i;
        if($i == "Value") value_col = i;
        if($i == "Units") units_col = i;
        if($i == "Variable_code") { var_code_col = i; $i="variable_code" }
    }
    # Print header row with renamed column
    print $year_col, $value_col, $units_col, $var_code_col;
}
NR>1 {
    # Print data rows with the correct columns
    print $year_col, $value_col, $units_col, $var_code_col;
}' raw/annual-enterprise-survey.csv > Transformed/2023_year_finance.csv

if [ -f transformed/2023_year_finance.csv ]; then
    echo "File successfully transformed and saved to the Transformed directory."
else
    echo "Failed to transform the file."
    exit 1
fi

# Step 3: Load
echo "Starting the load process..."
mkdir -p gold
cp Transformed/2023_year_finance.csv Gold/

if [ -f gold/2023_year_finance.csv ]; then
    echo "File successfully loaded into the Gold directory."
else
    echo "Failed to load the file."
    exit 1
fi

echo "ETL process completed successfully!"
