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
        # Accessing version triggers a system/info fetch internally
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
    """
    Fetches enrollments page-by-page.
    FIX: totalPages=false avoids the server-side 'unique result' 409 error.
    """
    all_instances = []
    page = 1
    page_size = 50

    while True:
        current_params = copy.deepcopy(params)
        current_params.update({
            'page': page,
            'pageSize': page_size,
            'totalPages': 'false'  # Critical fix for 409 error
        })

        resp_data = api.get('tracker/enrollments', params=current_params).json()
        instances = resp_data.get('instances', resp_data.get('enrollments', []))

        if not instances:
            break

        all_instances.extend(instances)

        # If we received fewer records than the page size, we have reached the end
        if len(instances) < page_size:
            break
        page += 1

    return all_instances


# ----------------------------
# 3. Import & Fallback Logic
# ----------------------------

def post_individual_teis(zebra_api, tei_list):
    """Fallback: Posts TEIs one by one if the batch post fails."""
    print(f"\n--- STARTING FALLBACK: POSTING {len(tei_list)} TEIs INDIVIDUALLY ---")
    success_count = 0
    for tei in tei_list:
        uid = tei.get('trackedEntity')
        single_payload = {'trackedEntities': [tei]}
        try:
            zebra_api.post('tracker', json=single_payload, params={
                'importStrategy': 'CREATE_AND_UPDATE',
                'async': 'false'
            })
            print(f"SUCCESS: TEI {uid} persisted.")
            success_count += 1
        except RequestException as e:
            try:
                err_data = json.loads(e.description)
                msg = err_data.get('message', 'No specific error message provided')
                print(f"FAILED: TEI {uid} | Reason: {msg}")
            except:
                print(f"FAILED: TEI {uid} | Status Code: {e.code}")
    return success_count > 0


def post_data_to_zebra(zebra_api, zebra_case_data):
    """POSTs batch payload with automatic individual fallback on failure."""
    batch_success = False
    try:
        response = zebra_api.post(
            'tracker',
            json=zebra_case_data,
            params={
                'async': 'false',
                'importStrategy': 'CREATE_AND_UPDATE',
                'reportMode': 'FULL',
                'atomicMode': 'OBJECT',
                'validationMode': 'SKIP'
            }
        )
        rj = response.json()
        stats = rj.get('stats', {})
        print(f"[ZEBRA] Batch Post Successful | created={stats.get('created', 0)}, updated={stats.get('updated', 0)}")
        batch_success = True
        return rj

    except RequestException as e:
        print(f"!!! BATCH POST FAILED (HTTP {e.code}). Attempting Fallback...")
        try:
            rj = json.loads(e.description)
            print(f"SERVER ERROR MESSAGE: {rj.get('message', 'Persistence failed')}")
        except:
            rj = {}

        tei_list = zebra_case_data.get('trackedEntities', [])
        if tei_list:
            batch_success = post_individual_teis(zebra_api, tei_list)
        return rj

    finally:
        if batch_success:
            run_zebra_analytics(zebra_api)


# ----------------------------
# 4. Core Helpers
# ----------------------------

def load_mappings():
    with open(MAPPING_FILE, 'r') as f:
        return json.load(f)["mappingDictionary"]


def check_ou_exists_in_zebra(zebra_api, ou_uid):
    """Queries target server to verify if the OU exists."""
    try:
        response = zebra_api.get(f'organisationUnits/{ou_uid}')
        return response.status_code == 200
    except RequestException:
        return False


def get_mapped_ou(source_ou_id, mappings):
    """Translates eIDSR OrgUnit and extracts clean 11-char UID."""
    ou_map = mappings.get("organisationUnits", {})
    if source_ou_id in ou_map:
        full_mapped_id = ou_map[source_ou_id]["mappedId"]
        return full_mapped_id.split('/')[-1]
    return None


def map_attributes(source_attrs, mappings, allowed_ids=None, log_warnings=False):
    """Maps attributes using Code-to-Code translation."""
    mapped = []
    tea_map = mappings.get("trackedEntityAttributesToTEI", {})
    raw_options = mappings.get("options", {})
    code_lookup = {opt["code"]: opt["mappedCode"] for opt in raw_options.values() if
                   "code" in opt and "mappedCode" in opt}

    for attr in source_attrs:
        src_id = attr.get('attribute')
        val = attr.get('value')
        if allowed_ids and src_id not in allowed_ids: continue
        if src_id in tea_map:
            target_id = tea_map[src_id]["mappedId"]
            mapped_val = code_lookup.get(val, val)
            mapped.append({"attribute": target_id, "value": mapped_val})
        elif log_warnings:
            print(f"Warning: TEA ID '{src_id}' is unmapped.")
    return mapped


# ----------------------------
# 5. Main Sync Workflow
# ----------------------------

def run_sync(period="today"):
    mappings = load_mappings()
    eidsr_api = Api.from_auth_file(EIDSR_AUTH)
    zebra_api = Api.from_auth_file(ZEBRA_AUTH)

    # 1. Capture Auth Exceptions and provide clear feedback
    if not check_auth(eidsr_api, "eIDSR") or not check_auth(zebra_api, "Zebra"):
        sys.exit(1)

    source_programs = [PROG_EBS, PROG_IBS]
    sync_queue = {}
    now = datetime.utcnow()

    # 2. Logic for today / this week / all time
    if period == "today":
        start_date = now.strftime('%Y-%m-%d')
    elif period == "this_week":
        start_date = (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d')
    else:
        start_date = "1900-01-01"

    for prog_id in source_programs:
        print(f"Sync: Fetching data for {prog_id} (After: {start_date})...")
        params = {'program': prog_id, 'ouMode': 'ALL', 'enrolledAfter': start_date}

        # 3. Handle Paging Safely
        instances = get_all_enrollments(eidsr_api, params)
        target_prog_id = mappings["trackerPrograms"][prog_id]["mappedId"]

        # Cache valid attributes for current program
        prog_info = eidsr_api.get(f'programs/{prog_id}', params={
            'fields': 'programTrackedEntityAttributes[trackedEntityAttribute[id]]'}).json()
        allowed_teas = {a['trackedEntityAttribute']['id'] for a in prog_info.get('programTrackedEntityAttributes', [])}

        for enr in instances:
            tei_id = enr['trackedEntity']
            if tei_id in sync_queue and prog_id == PROG_EBS: continue

            tei_full = eidsr_api.get(f'tracker/trackedEntities/{tei_id}',
                                     params={
                                         'fields': 'trackedEntity,orgUnit,attributes,enrollments[program,enrolledAt,attributes]'}).json()

            mapped_ou = get_mapped_ou(tei_full['orgUnit'], mappings)
            target_ou_to_use = mapped_ou if mapped_ou else tei_full['orgUnit']

            # 4. Verify OU exists on Zebra before continuing
            if not check_ou_exists_in_zebra(zebra_api, target_ou_to_use):
                print(f"SKIPPING TEI {tei_id}: OrgUnit '{target_ou_to_use}' not on Zebra.")
                continue

            mapped_main_attrs = map_attributes(tei_full.get('attributes', []), mappings, allowed_teas,
                                               log_warnings=True)

            target_enr_list = []
            for source_enr in tei_full.get('enrollments', []):
                if source_enr['program'] == prog_id:
                    z_enr_id = None
                    try:
                        z_check = zebra_api.get('tracker/enrollments', params={
                            'trackedEntity': tei_id, 'program': target_prog_id, 'orgUnit': target_ou_to_use
                        }).json().get('instances', [])
                        z_enr_id = z_check[0]['enrollment'] if z_check else None
                    except:
                        pass

                    target_enr_list.append({
                        "program": target_prog_id,
                        "enrollment": z_enr_id,
                        "orgUnit": target_ou_to_use,
                        "status": "ACTIVE",
                        "enrolledAt": source_enr['enrolledAt'],
                        "attributes": map_attributes(source_enr.get('attributes', []), mappings, allowed_teas,
                                                     log_warnings=False)
                    })

            sync_queue[tei_id] = {
                "trackedEntity": tei_id,
                "trackedEntityType": TE_TYPE_ZEBRA,
                "program": target_prog_id,
                "orgUnit": target_ou_to_use,
                "attributes": mapped_main_attrs,
                "enrollments": target_enr_list
            }

    if sync_queue:
        payload = {'trackedEntities': list(sync_queue.values())}
        with open(PAYLOAD_FILE, 'w') as f:
            json.dump(payload, f, indent=4)
        print(f"Posting {len(payload['trackedEntities'])} records to Zebra...")
        post_data_to_zebra(zebra_api, payload)
    else:
        print("No new data identified.")


if __name__ == "__main__":
    # 5. CLI Parameter Implementation
    parser = argparse.ArgumentParser(description="eIDSR to Zebra Sync")
    parser.add_argument("-p", "--period", choices=["today", "this_week", "all_time"], default="today")
    args = parser.parse_args()
    run_sync(period=args.period)
