import re
import html
import requests
import pandas as pd
from tqdm import tqdm
import time
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_KEY = 'f28399a0-0590-48ad-b00c-3d5483f78c9d'

SEARCH_URL = 'https://www.reed.co.uk/api/1.0/search'
DETAIL_URL = 'https://www.reed.co.uk/api/1.0/jobs/{job_id}'
PAGE_SIZE = 25
MAX_EMPTY_RESPONSES = 5

FINAL_FILE = 'reed_jobs_uk_extended.csv'
PARTIAL_FILE = 'reed_jobs_partial.csv'


def strip_html(text):
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html.unescape(text)
    return ' '.join(text.split())


def fetch_full_description(job_id):
    try:
        r = requests.get(
            DETAIL_URL.format(job_id=job_id),
            headers={'Accept': 'application/json'},
            auth=(API_KEY, ''),
            verify=False,
            timeout=10
        )
        if r.status_code == 200:
            raw = r.json().get('jobDescription', '')
            return strip_html(raw)
    except Exception:
        pass
    return ''


all_jobs = []
page = 1
empty_page_count = 0

progress = tqdm(desc='Fetching jobs', ncols=100)

try:
    while True:
        params = {
            'resultsToTake': PAGE_SIZE,
            'resultsToSkip': (page - 1) * PAGE_SIZE
        }

        r = requests.get(
            SEARCH_URL,
            headers={'Accept': 'application/json'},
            auth=(API_KEY, ''),
            params=params,
            verify=False,
            timeout=10
        )

        if r.status_code != 200:
            print(f"HTTP {r.status_code} on page {page}: {r.text}")
            break

        jobs = r.json().get('results', [])

        if not jobs:
            empty_page_count += 1
            if empty_page_count >= MAX_EMPTY_RESPONSES:
                break
        else:
            empty_page_count = 0

        for job in jobs:
            if job.get('jobId') and job.get('jobTitle'):
                job['jobDescription'] = fetch_full_description(job['jobId'])
                all_jobs.append(job)
                progress.update(1)
                time.sleep(0.2)

        pd.DataFrame(all_jobs).to_csv(PARTIAL_FILE, index=False)
        page += 1
        time.sleep(0.5)

except Exception as e:
    print(f"\nStopped on page {page}: {e}")

finally:
    progress.close()
    pd.DataFrame(all_jobs).to_csv(FINAL_FILE, index=False)
    print(f"{len(all_jobs)} jobs saved to {FINAL_FILE}")
