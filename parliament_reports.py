import json
import urllib.request
from datetime import datetime, timedelta
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


def get_existing_publication_ids(csv_filename):
    """
    Read the CSV file and return a set of existing publication IDs.
    
    Args:
        csv_filename: Path to CSV file
        
    Returns:
        set: Set of publication IDs already in the file
    """
    existing_ids = set()
    
    # Check if file exists
    if not os.path.isfile(csv_filename):
        return existing_ids
    
    # Read existing IDs from CSV
    with open(csv_filename, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            pub_id = row.get('Publication ID', '').strip()
            if pub_id:
                existing_ids.add(pub_id)
    
    return existing_ids


def get_scanned_publication_ids(scans_csv_filename):
    """
    Read the scans CSV file and return a set of all publication IDs ever scanned.
    
    Args:
        scans_csv_filename: Path to scans CSV file
        
    Returns:
        set: Set of all publication IDs that have been recorded in scans
    """
    scanned_ids = set()
    
    # Check if file exists
    if not os.path.isfile(scans_csv_filename):
        return scanned_ids
    
    # Read all IDs from scans CSV
    with open(scans_csv_filename, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            pub_ids_str = row.get('New Publication IDs', '').strip()
            if pub_ids_str:
                # Split by comma and add each ID to the set
                ids = [pid.strip() for pid in pub_ids_str.split(',')]
                scanned_ids.update(ids)
    
    return scanned_ids


def record_reports(csv_filename, rows_to_add):
    """
    Record reports in the reports CSV file.
    
    Args:
        csv_filename: Path to reports CSV file
        rows_to_add: List of row data to append
    """
    # Check if file exists to determine if we need to write headers
    file_exists = os.path.isfile(csv_filename)
    
    # Append to CSV file
    with open(csv_filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header only if file is new
        if not file_exists:
            writer.writerow([
                'Publication ID',
                'HC Number',
                'Session',
                'Committee Name',
                'House',
                'Report Title',
                'Report Ordinal',
                'Publication Date',
                'Publication Time',
                'Late by min',
                'Late by max'
            ])
        
        # Write data rows
        writer.writerows(rows_to_add)


def record_scan(scans_csv_filename, new_pub_ids):
    """
    Record a scan event in the scans CSV file.
    
    Args:
        scans_csv_filename: Path to scans CSV file
        new_pub_ids: List of new publication IDs found in this scan
    """
    # Check if file exists to determine if we need to write headers
    file_exists = os.path.isfile(scans_csv_filename)
    
    # Get current date and time
    scan_datetime = datetime.now()
    scan_date = scan_datetime.strftime('%Y-%m-%d')
    scan_time = scan_datetime.strftime('%H:%M:%S')
    
    # Format publication IDs as comma-separated string
    pub_ids_str = ', '.join(new_pub_ids) if new_pub_ids else ''
    
    # Append to scans CSV file
    with open(scans_csv_filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header only if file is new
        if not file_exists:
            writer.writerow(['Scan date', 'Scan time', 'New Publication IDs'])
        
        # Write scan record
        writer.writerow([scan_date, scan_time, pub_ids_str])


def calculate_lateness(csv_filename, scans_csv_filename):
    """
    Calculate 'Late by min' and 'Late by max' for reports that don't have these values yet.
    
    Args:
        csv_filename: Path to reports CSV file
        scans_csv_filename: Path to scans CSV file
    """
    # Check if files exist
    if not os.path.isfile(csv_filename) or not os.path.isfile(scans_csv_filename):
        return
    
    # Read all scan records
    scan_records = []
    with open(scans_csv_filename, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            scan_date = row.get('Scan date', '').strip()
            scan_time = row.get('Scan time', '').strip()
            pub_ids_str = row.get('New Publication IDs', '').strip()
            
            if scan_date and scan_time:
                try:
                    scan_datetime = datetime.strptime(f"{scan_date} {scan_time}", '%Y-%m-%d %H:%M:%S')
                    pub_ids = [pid.strip() for pid in pub_ids_str.split(',')] if pub_ids_str else []
                    scan_records.append({
                        'datetime': scan_datetime,
                        'pub_ids': pub_ids
                    })
                except ValueError:
                    continue
    
    # Read all report records
    reports = []
    with open(csv_filename, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            reports.append(row)
    
    # Process each report and calculate lateness if needed
    for report in reports:
        # Skip if 'Late by max' is already populated
        if report.get('Late by max', '').strip():
            continue
        
        pub_id = report.get('Publication ID', '').strip()
        pub_date = report.get('Publication Date', '').strip()
        pub_time = report.get('Publication Time', '').strip()
        
        if not pub_id or not pub_date or not pub_time:
            continue
        
        try:
            pub_datetime = datetime.strptime(f"{pub_date} {pub_time}", '%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
        
        # Find 'Late by max': first scan that includes this publication ID
        late_by_max = None
        for scan in scan_records:
            if pub_id in scan['pub_ids']:
                time_diff = scan['datetime'] - pub_datetime
                late_by_max = str(time_diff)
                break
        
        # Find 'Late by min': last scan before the one containing this ID that occurred after publication
        late_by_min = None
        last_scan_before = None
        
        for i, scan in enumerate(scan_records):
            if pub_id in scan['pub_ids']:
                # Found the scan with this pub_id, now look backwards
                for j in range(i - 1, -1, -1):
                    if scan_records[j]['datetime'] > pub_datetime:
                        last_scan_before = scan_records[j]
                    else:
                        break
                
                if last_scan_before:
                    time_diff = last_scan_before['datetime'] - pub_datetime
                    late_by_min = str(time_diff)
                break
        
        # Update the report record
        report['Late by max'] = late_by_max if late_by_max else ''
        report['Late by min'] = late_by_min if late_by_min else ''
    
    # Write updated reports back to CSV
    with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        fieldnames = [
            'Publication ID',
            'HC Number',
            'Session',
            'Committee Name',
            'House',
            'Report Title',
            'Report Ordinal',
            'Publication Date',
            'Publication Time',
            'Late by min',
            'Late by max'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reports)


def filter_and_process_reports(api_url, csv_filename, scans_csv_filename):
    """
    Fetch reports from API, filter by criteria, and append to CSV files.
    
    Args:
        api_url: API endpoint URL
        csv_filename: Path to CSV file to append data
        scans_csv_filename: Path to scans CSV file to record scan events
    """
    # Fetch data from API
    data = fetch_json_data(api_url)
    
    # Get all publication IDs that have ever been scanned
    scanned_ids = get_scanned_publication_ids(scans_csv_filename)
        
    # Get existing publication IDs from reports CSV
    existing_ids = get_existing_publication_ids(csv_filename)
    
    # Track new publication IDs found in this scan
    new_scan_ids = []
    
    # Prepare rows to add
    rows_to_add = []
    
    # Process each item
    for item in data.get('items', []):
        # Extract publication ID
        pub_id = str(item.get('id', ''))
        
        # Track if this is a new publication ID for scans
        if pub_id and pub_id not in scanned_ids:
            new_scan_ids.append(pub_id)
        
        # Skip if this publication ID already exists in reports CSV
        if pub_id in existing_ids:
            continue
        
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
        
        # Extract description and split into ordinal and title
        description = item.get('description', '')
        ordinal, title = split_report_title(description)
        
        # Extract other fields
        committee_name = committee.get('name', '')
        pub_date = pub_start_datetime.strftime('%Y-%m-%d')
        pub_time = pub_start_datetime.strftime('%H:%M:%S')
        
        # Extract HC number information
        hc_number_obj = item.get('hcNumber', {})
        hc_number = hc_number_obj.get('number', '') if hc_number_obj else ''
        session_description = hc_number_obj.get('sessionDescription', '') if hc_number_obj else ''
        
        # Add row (Late by min and Late by max will be calculated later)
        rows_to_add.append([
            pub_id,
            hc_number,
            session_description,
            committee_name,
            house,
            title,
            ordinal,
            pub_date,
            pub_time,
            '',  # Late by min (to be calculated)
            ''   # Late by max (to be calculated)
        ])
    
    # Record reports in reports CSV
    record_reports(csv_filename, rows_to_add)
    
    # Record this scan in scans CSV
    record_scan(scans_csv_filename, new_scan_ids)
    
    # Calculate lateness values
    calculate_lateness(csv_filename, scans_csv_filename)
    
    print(f"Added {len(rows_to_add)} reports to {csv_filename}")
    print(f"Recorded {len(new_scan_ids)} new publication IDs in {scans_csv_filename}")
    return len(rows_to_add)


if __name__ == '__main__':
    # API endpoint
    API_URL = 'https://committees-api.parliament.uk/api/Publications?PublicationTypeIds=1&SortOrder=PublicationDateDescending&Take=50&StartDate=2024-07-01'
    
    # CSV output files
    CSV_FILE = 'reports.csv'
    SCANS_CSV_FILE = 'scans.csv'
    
    # Run the script
    filter_and_process_reports(API_URL, CSV_FILE, SCANS_CSV_FILE)
