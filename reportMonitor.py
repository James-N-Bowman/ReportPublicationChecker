from __future__ import annotations
import csv
import json
import os
import re
import urllib.request
from datetime import datetime, date, time
from time import sleep
from typing import Dict, List, Optional, Union

import requests
from lxml import html

# ============================================================================
# CSV UTILITIES
# ============================================================================

def write_csv_file(filename: str, rows: List[Dict[str, str]]):
    """Write data to a CSV file."""
    if not rows:
        return
    
    # Ensure directory exists
    directory = os.path.dirname(filename)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    # Get headers from first row
    headers = list(rows[0].keys())
    
    # Write CSV file
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def read_csv_file(filename: str) -> List[Dict[str, str]]:
    """Read data from a CSV file."""
    if not os.path.isfile(filename):
        return []
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception:
        return []


def get_existing_ids_from_csv(filename: str, id_column: str) -> set:
    """Read CSV file and return set of existing IDs from specified column."""
    existing_ids = set()
    
    if not os.path.isfile(filename):
        return existing_ids
    
    rows = read_csv_file(filename)
    for row in rows:
        id_val = row.get(id_column, '').strip()
        if id_val:
            existing_ids.add(id_val)
    
    return existing_ids


# ============================================================================
# DATE/TIME PARSING UTILITIES
# ============================================================================

def _to_date(d: Union[str, datetime, date]) -> date:
    """
    Normalize input to a date object.
    Accepts:
      - datetime.date -> same day
      - datetime.datetime -> its date()
      - ISO-like strings e.g. 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS'
      - Common UK/US formats 'DD/MM/YYYY' or 'MM/DD/YYYY' (best effort)
    """
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        s = d.strip()
        # Try ISO formats first
        try:
            return datetime.fromisoformat(s).date()
        except ValueError:
            pass
        # Try a few common formats
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    raise ValueError(f"Could not parse date value: {d!r}")


# Precompile regexes for speed/readability
_TIME_RE = re.compile(
    r"""
    \b
    (?P<h>\d{1,2})                # hour
    (?:
        [:.]                      # separator : or .
        (?P<m>\d{2})              # minutes
    )?
    \s*
    (?P<ampm>(am|pm))?            # optional am/pm
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

_DATE_RE = re.compile(
    r"""
    ^\s*
    (?:(?:Mon|Tue|Tues|Wed|Thu|Thur|Thurs|Fri|Sat|Sun|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+)?
    (?P<day>\d{1,2})\s+
    (?P<month>[A-Za-z]+)
    (?:[,\s]+(?P<year>\d{4}))?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

_MONTHS = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'sept': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12,
}


def _parse_time(s: str):
    """Return a datetime.time if present in string, else empty string."""
    norm = s.replace("\u00B7", ".").replace(".", ":")
    m = _TIME_RE.search(norm)
    if not m:
        return ""
    
    h = int(m.group("h"))
    m_str = m.group("m")
    ampm = (m.group("ampm") or "").lower()
    mm = int(m_str) if m_str is not None else 0
    
    if not (0 <= h <= 23) and not ampm:
        return ""
    if not (0 <= mm <= 59):
        return ""
    
    if ampm == "pm" and h != 12:
        h = h + 12
    
    if not (0 <= h <= 23):
        return ""
    
    try:
        return time(hour=h, minute=mm)
    except ValueError:
        return ""


def _parse_date(s: str, op_date: date):
    """Return a datetime.date if present, else empty string."""
    cleaned = _TIME_RE.sub("", s.replace(".", ":"), count=1)
    cleaned = re.sub(r"\s*,\s*", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    
    m = _DATE_RE.match(cleaned)
    if not m:
        return ""
    
    day = int(m.group("day"))
    month_name = m.group("month").lower()
    month = _MONTHS.get(month_name[:3], _MONTHS.get(month_name))
    if not month:
        return ""
    
    year_str = m.group("year")
    if year_str:
        year = int(year_str)
    else:
        year = op_date.year
        candidate = date(year, month, day)
        if candidate < op_date:
            year += 1
    
    try:
        return date(year, month, day)
    except ValueError:
        return ""


# ============================================================================
# API & DATA FETCHING
# ============================================================================

def fetch_json_data(url: str, max_retries: int = 5, initial_delay: int = 1) -> Dict:
    """
    Fetch JSON data from URL with exponential backoff retry logic.
    """
    attempt = 0
    delay = initial_delay
    
    while attempt < max_retries:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                attempt += 1
                if attempt < max_retries:
                    sleep(delay)
                    delay *= 2
                else:
                    print(f"Max retries reached for {url}")
                    return {}
            else:
                print(f"Error fetching {url}: {response.status_code}")
                return {}
        except requests.RequestException as e:
            print(f"Request exception: {e}")
            return {}
    
    return {}


def fetch_document_html_as_lxml(doc_id: str):
    """
    Fetch the HTML for a business paper document from parliament.uk
    and return an lxml.html tree.
    """
    url = f"https://publications.parliament.uk/pa/cm/cmbusn/{doc_id}.htm"
    
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            raw_bytes = response.read()
            encoding = response.headers.get_content_charset() or 'utf-8'
            page_html = raw_bytes.decode(encoding, errors='replace')
        
        tree = html.fromstring(page_html)
        return tree
    
    except Exception as e:
        print(f"Error fetching document {doc_id}: {e}")
        return None


def get_document_id_for_date(
    target_date: Union[str, datetime, date],
    *,
    base_url: str = "https://housepapers-api.parliament.uk/api/document/?DocumentTypeId=1&skip={}",
    page_size: int = 20,
    timeout: int = 15,
    max_pages: int = 200,
    target_notes_text: str = "Today's business in the Chamber and Westminster Hall",
    retry_attempts: int = 3,
    retry_backoff: float = 0.8,
    session: Optional[requests.Session] = None,
) -> Optional[int]:
    """
    Page through House Papers API to find document Id for given date.
    Returns: int Id if found, else None.
    """
    target = _to_date(target_date)
    
    own_session = False
    if session is None:
        session = requests.Session()
        own_session = True
    
    headers = {
        "Accept": "application/json",
        "User-Agent": "HousePapersClient/1.0 (+https://example.org)",
    }
    
    try:
        skip = 0
        pages_visited = 0
        total_results = None
        closest_after = None  # Track (date, id) tuple for closest date after target
        
        while pages_visited < max_pages:
            url = base_url.format(skip)
            last_exc = None
            
            for attempt in range(1, retry_attempts + 1):
                try:
                    resp = session.get(url, headers=headers, timeout=timeout)
                    if resp.status_code in (429, 500, 502, 503, 504):
                        retry_after = resp.headers.get("Retry-After")
                        sleep_s = float(retry_after) if retry_after else retry_backoff * attempt
                        sleep(sleep_s)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except (requests.RequestException, ValueError) as e:
                    last_exc = e
                    if attempt == retry_attempts:
                        raise
                    sleep(retry_backoff * attempt)
            else:
                raise last_exc or RuntimeError("Unknown fetching error")
            
            if total_results is None:
                total_results = data.get("TotalResults")
            
            results = data.get("Results") or []
            if not results:
                return None
            
            for item in results:
                bd_raw = item.get("BusinessDate")
                try:
                    bd = _to_date(bd_raw) if bd_raw else None
                except ValueError:
                    bd = None
                
                if bd is None:
                    continue

                notes = item.get("Notes")
                    
                # If we've gone past target into older dates, stop searching
                if bd < target and notes is not None and target_notes_text in notes:
                    # Return exact match if found, otherwise closest after target
                    return closest_after[1] if closest_after else None
                
                # Exact match with correct notes - return immediately
                if bd == target and notes is not None and target_notes_text in notes:
                    the_id = item.get("Id")
                    return int(the_id) if the_id is not None else None
                
                # Track closest date after target (bd > target)
                if bd > target and notes is not None and target_notes_text in notes:
                    the_id = item.get("Id")
                    if the_id is not None:
                        # Keep the smallest date that's still greater than target
                        if closest_after is None or bd < closest_after[0]:
                            closest_after = (bd, int(the_id))
            
            skip += page_size
            pages_visited += 1
            
            if total_results is not None and skip >= total_results:
                return None
        
        return None
    
    finally:
        if own_session:
            session.close()


def split_report_title(text: str) -> tuple[str, str]:
    """
    Split a report description into ordinal (e.g. 'First Report')
    and the rest of the title.
    """
    pattern = r'^((?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|special)\s+report)\s+(.*)$'
    match = re.match(pattern, text.strip(), re.IGNORECASE)
    
    if match:
        ordinal = match.group(1)
        title = match.group(2)
        return ordinal, title
    
    return '', text


# ============================================================================
# DATA PROCESSING FUNCTIONS
# ============================================================================

def get_scanned_publication_ids(filename: str) -> set:
    """
    Extract all Publication IDs from the Scans CSV file.
    Returns: set of publication IDs (as strings)
    """
    scanned_ids = set()
    
    if not os.path.isfile(filename):
        return scanned_ids
    
    rows = read_csv_file(filename)
    for row in rows:
        ids_str = row.get('New Publication IDs', '').strip()
        if ids_str:
            ids_list = [x.strip() for x in ids_str.split(',') if x.strip()]
            scanned_ids.update(ids_list)
    
    return scanned_ids


def parse_committee_reports_published_today(tree, existing_order_papers: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Parse the 'Committee Reports Published Today' table from an order paper HTML.
    Returns: list of dictionaries with parsed data
    """
    if tree is None:
        print("Warning: HTML tree is None, cannot parse order papers")
        return []
    
    # Build set of existing (Committee Name, HC Number) pairs
    existing_pairs = set()
    for row in existing_order_papers:
        committee = row.get('Committee Name', '').strip()
        hc_num = row.get('HC Number', '').strip()
        if committee and hc_num:
            existing_pairs.add((committee, hc_num))
    
    tables = tree.xpath('//table')
    print(f"Found {len(tables)} tables in the HTML document")
    
    for idx, table in enumerate(tables):
        caption = table.xpath('.//caption/text()')
        caption_text = caption[0] if caption else "No caption"
        print(f"Table {idx + 1} caption: {caption_text}")
        
        if caption and 'Committee Reports Published Today' in caption[0]:
            print("Found 'Committee Reports Published Today' table!")
            
            op_date = None
            date_para = table.xpath('preceding-sibling::p[@class="Date"]')
            if date_para:
                date_text = date_para[-1].text_content().strip()
                print(f"Found date paragraph: {date_text}")
                op_date = _parse_date(date_text, date.today())
            
            if not op_date or op_date == "":
                op_date = date.today()
                print(f"Using today's date: {op_date}")
            
            rows_data = []
            rows = table.xpath('.//tr')[1:]  # Skip header row
            print(f"Found {len(rows)} data rows in table")
            
            for row_idx, row in enumerate(rows):
                cells = row.xpath('.//td')
                if len(cells) < 4:
                    print(f"Row {row_idx + 1}: Skipping (only {len(cells)} cells)")
                    continue
                
                committee_name = cells[0].text_content().strip()
                report_title = cells[1].text_content().strip()
                hc_number = cells[2].text_content().strip()
                date_str = cells[3].text_content().strip()
                
                print(f"Row {row_idx + 1}: {committee_name} | {hc_number}")
                
                # Skip if this pair already exists
                if (committee_name, hc_number) in existing_pairs:
                    print(f"  -> Skipping duplicate: ({committee_name}, {hc_number})")
                    continue
                
                pub_date = _parse_date(date_str, op_date)
                pub_time = _parse_time(date_str)
                
                if pub_date == "":
                    pub_date = op_date
                
                if pub_time == "":
                    pub_time = time(0, 1)
                
                rows_data.append({
                    'Committee Name': committee_name,
                    'Report Title': report_title,
                    'HC Number': hc_number,
                    'Publication date': str(pub_date),
                    'Publication time': str(pub_time),
                    'HC matched': ''
                })
            
            print(f"Parsed {len(rows_data)} new order paper entries")
            return rows_data
    
    print("Warning: 'Committee Reports Published Today' table not found in HTML")
    return []


def calculate_lateness(reports_file: str, scans_file: str) -> List[Dict[str, str]]:
    """
    Calculate lateness metrics for reports based on scan history.
    Returns: Updated reports list with lateness calculations
    """
    reports = read_csv_file(reports_file)
    scans = read_csv_file(scans_file)
    
    # Build scan records with datetime and pub_ids
    scan_records = []
    for scan_row in scans:
        scan_date = scan_row.get('Scan date', '').strip()
        scan_time = scan_row.get('Scan time', '').strip()
        
        if not scan_date or not scan_time:
            continue
        
        try:
            scan_datetime = datetime.strptime(f"{scan_date} {scan_time}", '%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
        
        ids_str = scan_row.get('New Publication IDs', '').strip()
        pub_ids = [x.strip() for x in ids_str.split(',') if x.strip()] if ids_str else []
        
        scan_records.append({
            'datetime': scan_datetime,
            'pub_ids': pub_ids
        })
    
    # Sort scan records by datetime
    scan_records.sort(key=lambda x: x['datetime'])
    
    # Calculate lateness for each report
    for report in reports:
        pub_id = report.get('Publication ID', '').strip()
        pub_date = report.get('Publication Date', '').strip()
        pub_time = report.get('Publication Time', '').strip()
        
        if not pub_date or not pub_time:
            continue
        
        try:
            pub_datetime = datetime.strptime(f"{pub_date} {pub_time}", '%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
        
        # Find 'Late by max'
        late_by_max = None
        for scan in scan_records:
            if pub_id in scan['pub_ids']:
                time_diff = scan['datetime'] - pub_datetime
                late_by_max = str(time_diff)
                break
        
        # Find 'Late by min'
        late_by_min = None
        last_scan_before = None
        
        for i, scan in enumerate(scan_records):
            if pub_id in scan['pub_ids']:
                for j in range(i - 1, -1, -1):
                    if scan_records[j]['datetime'] > pub_datetime:
                        last_scan_before = scan_records[j]
                    else:
                        break
                
                if last_scan_before:
                    time_diff = last_scan_before['datetime'] - pub_datetime
                    late_by_min = str(time_diff)
                break
        
        report['Late by max'] = late_by_max if late_by_max else ''
        report['Late by min'] = late_by_min if late_by_min else ''
    
    return reports


def match_order_papers_to_reports(order_papers: List[Dict[str, str]], 
                                   reports: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Match order papers to published reports by HC Number.
    Updates 'HC matched' field based on whether matching reports are found.
    
    Returns: Updated list of order paper dictionaries
    """
    # Build index of HC Numbers from Reports for fast lookup
    reports_hc_numbers = set()
    for report in reports:
        hc_num = report.get('HC Number', '').strip()
        if hc_num:
            reports_hc_numbers.add(hc_num)
    
    # Get current datetime for comparison
    now = datetime.now()
    
    # Process each order paper entry
    for op_row in order_papers:
        hc_matched = op_row.get('HC matched', '').strip()
        
        # Skip if already marked as Published or OP Error
        if hc_matched in ('Published', 'OP Error'):
            continue
        
        # Get HC Number from order paper
        op_hc_number = op_row.get('HC Number', '').strip()
        
        if not op_hc_number:
            continue
        
        # Check if this HC Number exists in Reports
        if op_hc_number in reports_hc_numbers:
            op_row['HC matched'] = 'Published'
        else:
            # No match found - check if publication time has passed
            pub_date_str = op_row.get('Publication date', '').strip()
            pub_time_str = op_row.get('Publication time', '').strip()
            
            if pub_date_str and pub_time_str:
                try:
                    # Parse publication datetime
                    pub_datetime = datetime.strptime(f"{pub_date_str} {pub_time_str}", '%Y-%m-%d %H:%M:%S')
                    
                    if now > pub_datetime:
                        op_row['HC matched'] = 'Missing'
                    else:
                        op_row['HC matched'] = 'Due'
                except ValueError:
                    # If we can't parse the date/time, leave HC matched as is
                    pass
    
    return order_papers


def filter_and_process_reports(api_url: str, reports_file: str, scans_file: str) -> tuple:
    """
    Fetch reports from API, filter by criteria, and prepare data for CSV.
    Returns: tuple of (new reports list, new scan IDs list)
    """
    data = fetch_json_data(api_url)
    
    scanned_ids = get_scanned_publication_ids(scans_file)
    existing_ids = get_existing_ids_from_csv(reports_file, 'Publication ID')
    
    new_scan_ids = []
    rows_to_add = []
    
    for item in data.get('items', []):
        pub_id = str(item.get('id', ''))
        
        if pub_id and pub_id not in scanned_ids:
            new_scan_ids.append(pub_id)
        
        if pub_id in existing_ids:
            continue
        
        committee = item.get('committee', {})
        house = committee.get('house', '')
        
        if house not in ['Commons', 'Joint']:
            continue
        
        pub_start_date_str = item.get('publicationStartDate', '')
        if not pub_start_date_str:
            continue
        
        try:
            pub_start_datetime = datetime.fromisoformat(pub_start_date_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            continue
        
        description = item.get('description', '')
        ordinal, title = split_report_title(description)
        
        committee_name = committee.get('name', '')
        pub_date = pub_start_datetime.strftime('%Y-%m-%d')
        pub_time = pub_start_datetime.strftime('%H:%M:%S')
        
        hc_number_obj = item.get('hcNumber', {})
        hc_number = hc_number_obj.get('number', '') if hc_number_obj else ''
        session_description = hc_number_obj.get('sessionDescription', '') if hc_number_obj else ''
        
        rows_to_add.append({
            'Publication ID': pub_id,
            'HC Number': hc_number,
            'Session': session_description,
            'Committee Name': committee_name,
            'House': house,
            'Report Title': title,
            'Report Ordinal': ordinal,
            'Publication Date': pub_date,
            'Publication Time': pub_time,
            'Late by min': '',
            'Late by max': ''
        })
    
    print(f"Found {len(rows_to_add)} new reports")
    print(f"Found {len(new_scan_ids)} new publication IDs")
    
    return rows_to_add, new_scan_ids


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == '__main__':
    # Define output directory and file paths
    OUTPUT_DIR = 'docs'
    REPORTS_FILE = os.path.join(OUTPUT_DIR, 'Reports.csv')
    SCANS_FILE = os.path.join(OUTPUT_DIR, 'Scans.csv')
    ORDER_PAPERS_FILE = os.path.join(OUTPUT_DIR, 'Order Papers.csv')
    
    # Collect all data
    all_reports = []
    all_scans = []
    all_order_papers = []
    
    # 1. Load existing data if files exist
    all_reports = read_csv_file(REPORTS_FILE)
    all_scans = read_csv_file(SCANS_FILE)
    all_order_papers = read_csv_file(ORDER_PAPERS_FILE)
    
    print(f"Loaded {len(all_reports)} existing reports")
    print(f"Loaded {len(all_scans)} existing scans")
    print(f"Loaded {len(all_order_papers)} existing order papers")
    
    # 2. Process committee reports from API
    API_URL = 'https://committees-api.parliament.uk/api/Publications?PublicationTypeIds=1&PublicationTypeIds=12&SortOrder=PublicationDateDescending'
    
    new_reports, new_scan_ids = filter_and_process_reports(
        API_URL, REPORTS_FILE, SCANS_FILE
    )
    
    # Add new reports
    all_reports.extend(new_reports)
    
    # Add scan record
    scan_datetime = datetime.now()
    scan_record = {
        'Scan date': scan_datetime.strftime('%Y-%m-%d'),
        'Scan time': scan_datetime.strftime('%H:%M:%S'),
        'New Publication IDs': ', '.join(new_scan_ids) if new_scan_ids else ''
    }
    all_scans.append(scan_record)
    
    # 3. Process order papers
    print("\n--- Processing Order Papers ---")
    doc_id = get_document_id_for_date(scan_datetime)
    if doc_id:
        print(f"Found document ID: {doc_id}")
        my_html = fetch_document_html_as_lxml(doc_id)
        if my_html is not None:
            op_data = parse_committee_reports_published_today(my_html, all_order_papers)
            if op_data:
                all_order_papers.extend(op_data)
                print(f"Added {len(op_data)} new order paper entries")
            else:
                print("No new order paper entries found")
        else:
            print("Failed to fetch HTML document")
    else:
        print("Could not find order paper document for specified date")
    
    # 4. Calculate lateness for all reports
    # First write intermediate files
    if all_reports:
        write_csv_file(REPORTS_FILE, all_reports)
    if all_scans:
        write_csv_file(SCANS_FILE, all_scans)
    
    # Now calculate lateness
    if all_scans and all_reports:
        all_reports = calculate_lateness(REPORTS_FILE, SCANS_FILE)
    
    # 5. Match order papers to reports
    if all_order_papers and all_reports:
        all_order_papers = match_order_papers_to_reports(all_order_papers, all_reports)
        print(f"Matched order papers to reports")
    
    # 6. Write final CSV files with all data
    if all_reports:
        write_csv_file(REPORTS_FILE, all_reports)
    if all_scans:
        write_csv_file(SCANS_FILE, all_scans)
    if all_order_papers:
        write_csv_file(ORDER_PAPERS_FILE, all_order_papers)
    
    print(f"\nData written to {OUTPUT_DIR} directory:")
    print(f"  - Reports.csv: {len(all_reports)} rows")
    print(f"  - Scans.csv: {len(all_scans)} rows")
    print(f"  - Order Papers.csv: {len(all_order_papers)} rows")
