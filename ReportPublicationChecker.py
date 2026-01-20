import json
import urllib.request
from datetime import datetime
import re
import csv
import os


def fetch_json_data(api_url):
    """Fetch JSON data from API endpoint"""
    with urllib.request.urlopen(api_url) as response:
        return json.loads(response.read().decode())


def split_report_title(input_string):
    """
    Split a report string into ordinal/type and title components.
    
    Args:
        input_string: String like "58th Report - blah blah blah"
        
    Returns:
        tuple: (report_prefix, title) or ("", original_string) if invalid
    """
    # Define valid dividers: hyphen, en-dash, em-dash, colon
    divider_pattern = r'\s*[-\u2013\u2014:]\s*'
    
    # Split on any of the valid dividers
    parts = re.split(divider_pattern, input_string, maxsplit=1)
    
    # Check if we got exactly 2 parts
    if len(parts) != 2:
        return ("", input_string)
    
    left_part = parts[0].strip()
    right_part = parts[1].strip()
    
    # Check if left part matches: ordinal number + "Report" or "Special Report"
    ordinal_pattern = r'^\d+(st|nd|rd|th)\s+(Special\s+)?Report$'
    
    if re.match(ordinal_pattern, left_part, re.IGNORECASE):
        return (left_part, right_part)
    else:
        return ("", input_string)


def filter_and_process_reports(api_url, csv_filename):
    """
    Fetch reports from API, filter by criteria, and append to CSV file.
    
    Args:
        api_url: API endpoint URL
        csv_filename: Path to CSV file to append data
    """
    # Fetch data from API
    data = fetch_json_data(api_url)
    
    # Get today's date (date only, no time)
    today = datetime.now().date()
    
    # Prepare rows to add
    rows_to_add = []
    
    # Process each item
    for item in data.get('items', []):
        # Extract committee house
        committee = item.get('committee', {})
        house = committee.get('house', '')
        
        # Filter: only Commons or Joint
        if house not in ['Commons', 'Joint']:
            continue
        
        # Extract and parse publication start date
        pub_start_date_str = item.get('publicationStartDate', '')
        if not pub_start_date_str:
            continue
        
        try:
            pub_start_datetime = datetime.fromisoformat(pub_start_date_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            continue
        
        # Filter: only today's date
        if pub_start_datetime.date() != today:
            continue
        
        # Extract description and split into ordinal and title
        description = item.get('description', '')
        ordinal, title = split_report_title(description)
        
        # Extract other fields
        committee_name = committee.get('name', '')
        pub_date = pub_start_datetime.strftime('%Y-%m-%d')
        pub_time = pub_start_datetime.strftime('%H:%M:%S')
        
        # Add row
        rows_to_add.append([
            committee_name,
            house,
            title,
            ordinal,
            pub_date,
            pub_time
        ])
    
    # Check if file exists to determine if we need to write headers
    file_exists = os.path.isfile(csv_filename)
    
    # Append to CSV file
    with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header only if file is new
        if not file_exists:
            writer.writerow([
                'Committee Name',
                'House',
                'Report Title',
                'Report Ordinal',
                'Publication Date',
                'Publication Time'
            ])
        
        # Write data rows
        writer.writerows(rows_to_add)
    
    print(f"Added {len(rows_to_add)} reports to {csv_filename}")
    return len(rows_to_add)


if __name__ == '__main__':
    # API endpoint
    API_URL = 'https://committees-api.parliament.uk/api/Publications?PublicationTypeIds=1&SortOrder=PublicationDateDescending&Take=50&StartDate=2024-07-01'
    
    # CSV output file
    CSV_FILE = 'reports.csv'
    
    # Run the script
    filter_and_process_reports(API_URL, CSV_FILE)
