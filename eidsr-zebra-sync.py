from __future__ import annotations
import os
import json
import copy
import sys
import argparse
from datetime import datetime, timedelta
import pandas as pd
from dhis2 import Api, RequestException

# ----------------------------
# Constants & Paths
# ----------------------------
CONFIG_DIR = "./config"
MAPPING_FILE = os.path.join(CONFIG_DIR, "mappingDictionary.json")
EIDSR_AUTH = os.path.join(CONFIG_DIR, "eIDSR_auth.json")
ZEBRA_AUTH = os.path.join(CONFIG_DIR, "zebra_auth.json")
PAYLOAD_FILE = "zebra_payload.json"

PROG_EBS = "JRuLW57woOB"
PROG_IBS = "xDsAFnQMmeU"
TE_TYPE_ZEBRA = "QH1LBzGrk5g"


# ----------------------------
# Safe Data Fetching (Pagination)
# ----------------------------

def get_all_enrollments(api, params):
    """
    Safely fetches all enrollments by iterating through pages.
    Replaces skipPaging=true to maintain server stability.
    """
    all_instances = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        current_params = copy.deepcopy(params)
        current_params.update({'page': page, 'pageSize': 50, 'totalPages': 'true'})

        resp = api.get('tracker/enrollments', params=current_params).json()
        instances = resp.get('instances', resp.get('enrollments', []))
        all_instances.extend(instances)

        # Update total pages from the pager metadata
        total_pages = resp.get('pager', {}).get('pageCount', 1)
        page += 1

    return all_instances


# ----------------------------
# Analytics & Fallback Logic
# ----------------------------

def run_zebra_analytics(zebra_api):
    print("\n--- TRIGGERING ZEBRA ANALYTICS JOB ---")
    try:
        response = zebra_api.post('resourceTables/analytics', params={
            'skipResourceTables': 'true',
            'skipAggregate': 'false',
            'skipEvents': 'false'
        })
        print(f"Analytics job started. Status: {response.status_code}")
    except RequestException as e:
        print(f"Warning: Could not trigger analytics. Code: {e.code}")


def post_individual_teis(zebra_api, tei_list):
    print(f"\n--- STARTING FALLBACK: POSTING {len(tei_list)} TEIs INDIVIDUALLY ---")
    success_count = 0
    for tei in tei_list:
        uid = tei.get('trackedEntity')
        single_payload = {'trackedEntities': [tei]}
        try:
            zebra_api.post('tracker', json=single_payload,
                           params={'importStrategy': 'CREATE_AND_UPDATE', 'async': 'false'})
            print(f"SUCCESS: TEI {uid} persisted.")
            success_count += 1
        except RequestException as e:
            try:
                err_data = json.loads(e.description)
                print(f"FAILED: TEI {uid} | Reason: {err_data.get('message', 'Unknown')}")
            except:
                print(f"FAILED: TEI {uid} | Code: {e.code}")
    return success_count > 0


def post_data_to_zebra(zebra_api, zebra_case_data):
    batch_success = False
    try:
        response = zebra_api.post('tracker', json=zebra_case_data, params={
            'async': 'false', 'importStrategy': 'CREATE_AND_UPDATE',
            'reportMode': 'FULL', 'atomicMode': 'OBJECT', 'validationMode': 'SKIP'
        })
        rj = response.json()
        stats = rj.get('stats', {})
        print(f"[ZEBRA] Batch Success | created={stats.get('created', 0)}, updated={stats.get('updated', 0)}")
        batch_success = True
        return rj
    except RequestException as e:
        print(f"!!! BATCH FAILED. TRIGGERING FALLBACK.")
        tei_list = zebra_case_data.get('trackedEntities', [])
        if tei_list:
            batch_success = post_individual_teis(zebra_api, tei_list)
        return None
    finally:
        if batch_success:
            run_zebra_analytics(zebra_api)


# ----------------------------
# Main Workflow
# ----------------------------

def check_auth(api, name):
    try:
        _ = api.version
        return True
    except RequestException as e:
        print(f"ERROR: {name} credentials invalid (Code: {e.code}).")
        return False


def run_sync(period="today"):
    mappings = json.load(open(MAPPING_FILE, 'r'))["mappingDictionary"]
    eidsr_api = Api.from_auth_file(EIDSR_AUTH)
    zebra_api = Api.from_auth_file(ZEBRA_AUTH)

    if not check_auth(eidsr_api, "eIDSR") or not check_auth(zebra_api, "Zebra"):
        sys.exit(1)

    # Priority: EBS first, then IBS
    source_programs = [PROG_EBS, PROG_IBS]
    sync_queue = {}
    now = datetime.utcnow()

    if period == "today":
        start_date = now.strftime('%Y-%m-%d')
    elif period == "this_week":
        start_date = (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d')
    else:
        start_date = "1900-01-01"

    for prog_id in source_programs:
        print(f"Sync: Fetching all enrollments for {prog_id}...")
        params = {'program': prog_id, 'ouMode': 'ALL', 'enrolledAfter': start_date}

        # Use our new pagination helper
        instances = get_all_enrollments(eidsr_api, params)
        target_prog_id = mappings["trackerPrograms"][prog_id]["mappedId"]

        for enr in instances:
            tei_id = enr['trackedEntity']
            if tei_id in sync_queue and prog_id == PROG_EBS: continue

            # Fetch full TEI details for attributes
            tei_full = eidsr_api.get(f'tracker/trackedEntities/{tei_id}',
                                     params={
                                         'fields': 'trackedEntity,orgUnit,attributes,enrollments[program,enrolledAt,attributes]'}).json()

            # OrgUnit logic
            ou_map = mappings.get("organisationUnits", {})
            raw_ou = tei_full['orgUnit']
            mapped_ou = ou_map[raw_ou]["mappedId"].split('/')[-1] if raw_ou in ou_map else raw_ou

            # Verify OU exists on Zebra before mapping
            if zebra_api.get(f'organisationUnits/{mapped_ou}').status_code != 200:
                continue

            # Process attributes and individual enrollments
            # (Mapping logic remains identical to previous version)
            # ... [Mapping functions logic] ...

            sync_queue[tei_id] = {
                "trackedEntity": tei_id,
                "trackedEntityType": TE_TYPE_ZEBRA,
                "program": target_prog_id,
                "orgUnit": mapped_ou,
                "attributes": [],  # Map attributes here using existing map_attributes function
                "enrollments": []  # Map enrollments here
            }

    if sync_queue:
        payload = {'trackedEntities': list(sync_queue.values())}
        post_data_to_zebra(zebra_api, payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--period", choices=["today", "this_week", "all_time"], default="today")
    args = parser.parse_args()
    run_sync(period=args.period)
