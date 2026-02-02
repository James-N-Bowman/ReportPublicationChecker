from __future__ import annotations
import base64
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

def write_csv(filename: str, rows: List[Dict[str, str]]):
    """Write data to a CSV file."""
    if not rows:
        return
    
    headers = list(rows[0].keys())
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(filename: str) -> List[Dict[str, str]]:
    """Read data from a CSV file."""
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception:
        return []


def get_existing_ids_from_csv(filename: str, id_column: str) -> set:
    """Read CSV file and return set of existing IDs from specified column."""
    existing_ids = set()

    if not os.path.isfile(filename):
        return existing_ids

    rows = read_csv(filename)
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
    year = int(year_str) if year_str else op_date.year

    try:
        return date(year, month, day)
    except ValueError:
        return ""


def parse_date_time(text: str, op_date: date):
    """Parse string containing date and/or time. Returns [date_obj, time_obj_or_empty_str]."""
    if not isinstance(text, str):
        raise TypeError("text must be a string")

    t = _parse_time(text)
    d = _parse_date(text, op_date)

    if d == "":
        d = op_date

    return [d, t]


# ============================================================================
# HOUSE PAPERS API
# ============================================================================

class HtmlFetchError(RuntimeError):
    """Raised when HTML cannot be fetched or decoded."""


def get_document_id_for_date(
    target_date: Union[str, datetime, date],
    *,
    base_url: str = "https://housepapers-api.parliament.uk/api/document/?DocumentTypeId=1&skip={}",
    page_size: int = 20,
    timeout: int = 15,
    max_pages: int = 200,
    notes_text: str = "Today's business in the Chamber and Westminster Hall.",
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

                if bd < target:
                    return None

                if bd == target and (item.get("Notes") or "") == notes_text:
                    the_id = item.get("Id")
                    return int(the_id) if the_id is not None else None

            skip += page_size
            pages_visited += 1

            if total_results is not None and skip >= total_results:
                return None

        return None

    finally:
        if own_session:
            session.close()


def fetch_document_html_as_lxml(
    doc_id: Union[int, str],
    *,
    timeout: int = 15,
    retry_attempts: int = 3,
    retry_backoff: float = 0.8,
    session: Optional[requests.Session] = None,
) -> html.HtmlElement:
    """
   Fetch Base64-encoded HTML for House Papers document and parse with lxml.
   Returns: lxml.html.HtmlElement
   Raises: HtmlFetchError
   """
    url = f"https://housepapers-api.parliament.uk/api/document/{doc_id}/html"

    own_session = False
    if session is None:
        session = requests.Session()
        own_session = True

    headers = {
        "Accept": "text/plain, application/octet-stream, application/json;q=0.9, */*;q=0.1",
        "User-Agent": "HousePapersClient/1.0 (+https://example.org)",
    }

    try:
        last_exc: Optional[Exception] = None
        for attempt in range(1, retry_attempts + 1):
            try:
                resp = session.get(url, headers=headers, timeout=timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    sleep_s = float(retry_after) if retry_after else retry_backoff * attempt
                    sleep(sleep_s)
                    continue

                resp.raise_for_status()
                content = resp.content

                if content.strip().startswith(b"{") and content.strip().endswith(b"}"):
                    try:
                        data = resp.json()
                        for key in ("html", "Html", "content", "Content", "data", "Data"):
                            if key in data and isinstance(data[key], str):
                                content = data[key].encode("utf-8")
                                break
                        else:
                            raise HtmlFetchError("Response is JSON but missing HTML Base64 field.")
                    except ValueError as e:
                        raise HtmlFetchError(f"Expected JSON but failed to parse: {e}") from e

                b64_bytes = b"".join(content.split())
                try:
                    html_bytes = base64.b64decode(b64_bytes, validate=True)
                except Exception as e:
                    raise HtmlFetchError(f"Failed to decode Base64 HTML: {e}") from e

                try:
                    return html.fromstring(html_bytes)
                except Exception as e:
                    raise HtmlFetchError(f"Failed to parse HTML with lxml: {e}") from e

            except (requests.RequestException, HtmlFetchError) as e:
                last_exc = e
                if attempt == retry_attempts:
                    break
                sleep(retry_backoff * attempt)

        raise HtmlFetchError(f"Failed to fetch/parse HTML for id {doc_id}: {last_exc}")

    finally:
        if own_session:
            session.close()


def parse_committee_reports_published_today(
    doc: html.HtmlElement,
    existing_order_papers: List[Dict[str, str]] = None
) -> Optional[List[Dict[str, str]]]:
    """
   Parse 'Committee Reports Published Today' section from lxml HTML element.
   Returns: list of dictionaries with report data, or None if section not found.
   Filters out duplicates based on existing order papers.
   """
    if existing_order_papers is None:
        existing_order_papers = []

    # Build set of existing (Order Paper date, HC Number) tuples for deduplication
    existing_keys = set()
    for op in existing_order_papers:
        op_date = op.get('Order Paper date', '').strip()
        hc_num = op.get('HC Number', '').strip()
        if op_date and hc_num:
            existing_keys.add((op_date, hc_num))

    def _norm(s: str | None) -> str:
        return " ".join((s or "").split())

    # Locate the <h1> to extract order paper date
    op_date = None
    prefix = "Order Paper for "
    for h1 in doc.xpath("//h1"):
        text = _norm(h1.xpath("string(.)"))
        if text.startswith(prefix):
            op_date_str = text[len(prefix):].strip()
            op_date_str = re.sub(r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+", 
                                 "", op_date_str, flags=re.IGNORECASE)
            op_date = datetime.strptime(op_date_str, "%d %B %Y").date()
            break

    if op_date is None:
        return None

    # Locate the <h3> section
    target_h3 = None
    for h3 in doc.xpath("//h3"):
        if _norm(h3.xpath("string(.)")) == "Committee Reports Published Today":
            target_h3 = h3
            break

    if target_h3 is None:
        return None

    results: List[Dict[str, str]] = []
    sib = target_h3.getnext()

    while sib is not None and (not isinstance(sib.tag, str) or sib.tag.lower() not in ("h3", "h2", "h1")):
        if isinstance(sib.tag, str) and sib.tag.lower() == "h5":
            committee = _norm(sib.text_content())

            p = sib.getnext()
            while p is not None and (not isinstance(p.tag, str) or p.tag.lower() != "p"):
                if isinstance(p.tag, str) and p.tag.lower() in ("h5", "h3", "h2", "h1"):
                    p = None
                    break
                p = p.getnext()

            if p is None:
                sib = sib.getnext()
                continue

            # Extract report description
            strongs = p.xpath(".//strong")
            report_description = _norm("".join(strongs[0].itertext())) if strongs else ""

            # Extract publication date/time
            op_rep_time = ""
            op_rep_date = ""
            if len(strongs) >= 2:
                datetime_string = _norm("".join(strongs[1].itertext()))
                op_rep_date, op_rep_time = parse_date_time(datetime_string, op_date)

            # Extract HC number
            hc_number = ""
            for sp in p.xpath('.//span[contains(@class, "Roman")]'):
                txt = _norm("".join(sp.itertext()))
                if txt.startswith("HC "):
                    hc_number = txt
                    break

            # Check if this combination already exists
            op_date_str = str(op_date)
            if (op_date_str, hc_number) not in existing_keys:
                item = {
                    'Order Paper date': op_date_str,
                    'Committee name': committee,
                    'Report description': report_description,
                    'HC Number': hc_number,
                    'Publication date': str(op_rep_date),
                    'Publication time': str(op_rep_time),
                    'HC matched': ''
                }
                results.append(item)

        sib = sib.getnext()

    return results


# ============================================================================
# COMMITTEES API
# ============================================================================

def fetch_json_data(api_url: str):
    """Fetch JSON data from API endpoint."""
    with urllib.request.urlopen(api_url) as response:
        return json.loads(response.read().decode())


def split_report_title(input_string: str):
    """
   Split report string into ordinal/type and title components.
   Returns: (report_prefix, title) or ("", original_string) if invalid
   """
    divider_pattern = r'\s*[-\u2013\u2014:]\s*'
    parts = re.split(divider_pattern, input_string, maxsplit=1)

    if len(parts) != 2:
        return ("", input_string)

    left_part = parts[0].strip()
    right_part = parts[1].strip()

    ordinal_pattern = r'^\d+(st|nd|rd|th)\s+(Special\s+)?Report$'

    if re.match(ordinal_pattern, left_part, re.IGNORECASE):
        return (left_part, right_part)
    else:
        return ("", input_string)


def get_scanned_publication_ids(filename: str) -> set:
    """
   Read scans CSV and return set of all publication IDs ever scanned.
   """
    scanned_ids = set()

    if not os.path.isfile(filename):
        return scanned_ids

    rows = read_csv(filename)
    for row in rows:
        pub_ids_str = row.get('New Publication IDs', '').strip()
        if pub_ids_str:
            ids = [pid.strip() for pid in pub_ids_str.split(',')]
            scanned_ids.update(ids)

    return scanned_ids


def calculate_lateness(reports_file: str, scans_file: str) -> List[Dict[str, str]]:
    """
   Calculate 'Late by min' and 'Late by max' for reports without these values.
   Returns: Updated list of report dictionaries
   """
    if not os.path.isfile(scans_file):
        return []

    # Read scan records
    scan_records = []
    scan_rows = read_csv(scans_file)
    for row in scan_rows:
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

    # Read and update report records
    reports = read_csv(reports_file)

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


def filter_and_process_reports(api_url: str, reports_file: str, scans_file: str) -> int:
    """
   Fetch reports from API, filter by criteria, and prepare data for CSV.
   Returns: number of new reports added
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
    REPORTS_FILE = 'docs/Reports.csv'
    SCANS_FILE = 'docs/Scans.csv'
    ORDER_PAPERS_FILE = 'docs/Order Papers.csv'

    # Collect all data
    all_reports = []
    all_scans = []
    all_order_papers = []

    # 1. Load existing data if files exist
    if os.path.isfile(REPORTS_FILE):
        all_reports = read_csv(REPORTS_FILE)
    if os.path.isfile(SCANS_FILE):
        all_scans = read_csv(SCANS_FILE)
    if os.path.isfile(ORDER_PAPERS_FILE):
        all_order_papers = read_csv(ORDER_PAPERS_FILE)

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
    doc_id = get_document_id_for_date(scan_datetime)
    if doc_id:
        my_html = fetch_document_html_as_lxml(doc_id)
        op_data = parse_committee_reports_published_today(my_html, all_order_papers)
        if op_data:
            all_order_papers.extend(op_data)
            print(f"Added {len(op_data)} new order paper entries")
    else:
        print("Could not find order paper document for specified date")

    # 4. Calculate lateness for all reports (creates updated reports list)
    if all_scans and all_reports:
        # Write intermediate files for calculate_lateness to read
        write_csv(REPORTS_FILE, all_reports)
        write_csv(SCANS_FILE, all_scans)

        # Now calculate lateness
        all_reports = calculate_lateness(REPORTS_FILE, SCANS_FILE)

    # 5. Match order papers to reports
    if all_order_papers and all_reports:
        all_order_papers = match_order_papers_to_reports(all_order_papers, all_reports)
        print(f"Matched order papers to reports")

    # 6. Write final CSV files with all data
    if all_reports:
        write_csv(REPORTS_FILE, all_reports)
    if all_scans:
        write_csv(SCANS_FILE, all_scans)
    if all_order_papers:
        write_csv(ORDER_PAPERS_FILE, all_order_papers)

    print(f"\nData written to CSV files:")
    print(f"  - {REPORTS_FILE}: {len(all_reports)} rows")
    print(f"  - {SCANS_FILE}: {len(all_scans)} rows")
    print(f"  - {ORDER_PAPERS_FILE}: {len(all_order_papers)} rows")
