# Healthcare Eligibility Pipeline

## Overview
This pipeline standardizes member eligibility data from different healthcare partners into a single, clean dataset. It uses a configuration-driven approach so you can add new partners without changing the core code.

## How to Run
1. **Prerequisites**: Ensure you have Python and PySpark installed.
2. **Files**: Put your data files (like `acme.txt` and `bettercare.csv`) in the project folder.
3. **Execute**: Run the script: `python pipeline_script.py`
4. **Results**: The script will clean the data and show a unified table.

## Adding a New Partner
To add a new data source, you only need to update the configuration file:
* Add the new partner’s delimiter (e.g., comma or pipe).
* Map their column names to our standard fields (e.g., their "ID" -> our "external_id").
* No core code changes are required.

## Standard Rules Applied
The pipeline automatically cleans the data as follows:
* **external_id**: Mapped from the partner's unique ID field.
* **first_name / last_name**: Converted to Title Case.
* **dob**: Formatted as ISO-8601 (YYYY-MM-DD).
* **email**: Forced to lowercase.
* **phone**: Standardized to XXX-XXX-XXXX.
* **partner_code**: Each row is tagged with its source identifier.