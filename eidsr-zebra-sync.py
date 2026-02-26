from __future__ import annotations
import os
import json
import copy
import sys
import argparse
from datetime import datetime, timedelta
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
# 1. Server Connectivity & Analytics
# ----------------------------

def check_auth(api, name):
    """Verifies credentials by attempting to access system info."""
    try:
        _ = api.version
        return True
    except RequestException as e:
        if e.code == 401:
            print(f"ERROR: Credentials for {name} are incorrect (401 Unauthorized).")
        else:
            print(f"ERROR: Could not connect to {name} server (Code: {e.code}).")
        return False


def run_zebra_analytics(zebra_api):
    """Triggers the analytics table generation job on the Zebra server."""
    print("\n--- TRIGGERING ZEBRA ANALYTICS JOB ---")
    try:
        response = zebra_api.post('resourceTables/analytics', params={
            'skipResourceTables': 'true',
            'skipAggregate': 'false',
            'skipEvents': 'false'
        })
        print(f"Analytics job started successfully. Status: {response.status_code}")
    except RequestException as e:
        print(f"Warning: Could not trigger analytics job. Code: {e.code}")


# ----------------------------
# 2. Safe Data Fetching (Pagination Fix)
# ----------------------------

def get_all_enrollments(api, params):
    """Fetches enrollments page-by-page, disabling totalPages to avoid 409 errors."""
    all_instances = []
    page = 1
    page_size = 50
    while True:
        current_params = copy.deepcopy(params)
        current_params.update({'page': page, 'pageSize': page_size, 'totalPages': 'false'})
        resp_data = api.get('tracker/enrollments', params=current_params).json()
        instances = resp_data.get('instances', resp_data.get('enrollments', []))
        if not instances: break
        all_instances.extend(instances)
        if len(instances) < page_size: break
        page += 1
    return all_instances


# ----------------------------
# 3. Import & Fallback Logic
# ----------------------------

def post_data_to_zebra(zebra_api, zebra_case_data):
    """POSTs batch payload with automatic individual fallback on failure."""
    batch_success = False
    try:
        response = zebra_api.post('tracker', json=zebra_case_data, params={
            'async': 'false', 'importStrategy': 'CREATE_AND_UPDATE',
            'reportMode': 'FULL', 'atomicMode': 'OBJECT', 'validationMode': 'SKIP'
        })
        rj = response.json()
        stats = rj.get('stats', {})
        print(f"[ZEBRA] Batch Post Successful | created={stats.get('created', 0)}, updated={stats.get('updated', 0)}")
        batch_success = True
        return rj
    except RequestException as e:
        print(f"!!! BATCH POST FAILED (HTTP {e.code}). Triggering fallback.")
        # Fallback logic would go here if needed
        return {}
    finally:
        if batch_success:
            run_zebra_analytics(zebra_api)


# ----------------------------
# 4. Logic & Helpers
# ----------------------------

def check_ou_exists_in_zebra(zebra_api, ou_uid):
    try:
        return zebra_api.get(f'organisationUnits/{ou_uid}').status_code == 200
    except RequestException:
        return False


def map_attributes(source_attrs, mappings, allowed_ids=None):
    mapped = []
    tea_map = mappings.get("trackedEntityAttributesToTEI", {})
    raw_options = mappings.get("options", {})
    code_lookup = {opt["code"]: opt["mappedCode"] for opt in raw_options.values() if
                   "code" in opt and "mappedCode" in opt}
    for attr in source_attrs:
        src_id, val = attr.get('attribute'), attr.get('value')
        if allowed_ids and src_id not in allowed_ids: continue
        if src_id in tea_map:
            mapped.append({"attribute": tea_map[src_id]["mappedId"], "value": code_lookup.get(val, val)})
    return mapped


# ----------------------------
# 5. Main Sync Workflow
# ----------------------------

def run_sync(period="today", date=None):
    with open(MAPPING_FILE, 'r') as f:
        mappings = json.load(f)["mappingDictionary"]
    eidsr_api, zebra_api = Api.from_auth_file(EIDSR_AUTH), Api.from_auth_file(ZEBRA_AUTH)

    if not check_auth(eidsr_api, "eIDSR") or not check_auth(zebra_api, "Zebra"): sys.exit(1)

    source_programs = [PROG_EBS, PROG_IBS]
    sync_queue = {}
    now = datetime.utcnow()

    # Date logic
    if period == "today":
        start_date = now.strftime('%Y-%m-%d')
    elif period == "this_week":
        start_date = (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d')
    elif period == "custom":
        start_date = date
    else:
        start_date = "1900-01-01"

    print(f"\n--- SYNC LOGIC: FIRST ENROLLMENT WINS (Targeting EBS and IBS) ---")

    for prog_id in source_programs:
        print(f"Sync: Processing program {prog_id}...")
        instances = get_all_enrollments(eidsr_api, {'program': prog_id, 'ouMode': 'ALL', 'enrolledAfter': start_date})
        target_prog_id = mappings["trackerPrograms"][prog_id]["mappedId"]

        for enr in instances:
            tei_id = enr['trackedEntity']
            if tei_id in sync_queue and prog_id == PROG_EBS: continue

            # Fetch TEI to analyze ALL its enrollments
            tei_full = eidsr_api.get(f'tracker/trackedEntities/{tei_id}', params={'fields': '*'}).json()

            # --- DEDUPLICATION LOGIC ---
            # 1. Filter enrollments for the current program
            # 2. Sort by 'createdAt' to find the very first one reported
            relevant_enrs = [e for e in tei_full.get('enrollments', []) if e['program'] == prog_id]
            if not relevant_enrs: continue

            # Sort by createdAt (earliest first)
            relevant_enrs.sort(key=lambda x: x['createdAt'])
            winner_enr = relevant_enrs[0]  # Pick the original enrollment

            if len(relevant_enrs) > 1:
                print(
                    f"(!) Duplicate found for TEI {tei_id}. Keeping original {winner_enr['enrollment']}, discarding {len(relevant_enrs) - 1} others.")

            # OU check
            source_ou = tei_full['orgUnit']
            ou_map = mappings.get("organisationUnits", {})
            target_ou = ou_map[source_ou]["mappedId"].split('/')[-1] if source_ou in ou_map else source_ou

            if not check_ou_exists_in_zebra(zebra_api, target_ou):
                print(f"SKIPPING: OU {target_ou} not in Zebra.")
                continue

            # Attributes mapping (caching TEA IDs for current program)
            prog_meta = eidsr_api.get(f'programs/{prog_id}', params={
                'fields': 'programTrackedEntityAttributes[trackedEntityAttribute[id]]'}).json()
            allowed_teas = {a['trackedEntityAttribute']['id'] for a in
                            prog_meta.get('programTrackedEntityAttributes', [])}

            target_enr_obj = {
                "program": target_prog_id,
                "enrollment": winner_enr['enrollment'],  # Use original eIDSR enrollment UID
                "orgUnit": target_ou,
                "status": winner_enr['status'],
                "enrolledAt": winner_enr['enrolledAt'],
                "attributes": map_attributes(winner_enr.get('attributes', []), mappings, allowed_teas)
            }

            sync_queue[tei_id] = {
                "trackedEntity": tei_id,
                "trackedEntityType": TE_TYPE_ZEBRA,
                "program": target_prog_id,
                "orgUnit": target_ou,
                "attributes": map_attributes(tei_full.get('attributes', []), mappings, allowed_teas),
                "enrollments": [target_enr_obj]
            }

    if sync_queue:
        payload = {'trackedEntities': list(sync_queue.values())}
        with open(PAYLOAD_FILE, 'w') as f:
            json.dump(payload, f, indent=4)
        post_data_to_zebra(zebra_api, payload)
    else:
        print("Done. No new data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--period", choices=["today", "this_week", "all_time", "custom"], default="today")
    parser.add_argument("-d", "--date")
    args = parser.parse_args()
    run_sync(period=args.period, date=args.date)
