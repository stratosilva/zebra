from __future__ import annotations
import os
import json
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
# Analytics Logic
# ----------------------------

def run_zebra_analytics(zebra_api):
    """
    Triggers the analytics table generation job on the Zebra server.
    Ensures that synced data appears in dashboards and reports.
    """
    print("\n--- TRIGGERING ZEBRA ANALYTICS JOB ---")
    try:
        # Triggering analytics tables generation
        response = zebra_api.post('resourceTables/analytics', params={
            'skipResourceTables': 'true',
            'skipAggregate': 'false',
            'skipEvents': 'false'
        })
        print(f"Analytics job started successfully. Status: {response.status_code}")
        print(f"Server Message: {response.json().get('message', 'Processing...')}")
    except RequestException as e:
        print(f"Warning: Could not trigger analytics job. Code: {e.code}")


# ----------------------------
# Fallback & Response Logic
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
    """POST payload to Zebra with advanced error analysis and individual fallback."""
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
        print(f"!!! BATCH POST FAILED (HTTP {e.code})")
        try:
            rj = json.loads(e.description)
            server_msg = rj.get('message', 'No message body found')
            print(f"SERVER ERROR MESSAGE: {server_msg}")
        except:
            print(f"Could not parse error body: {e.description}")
            rj = {}

        tei_list = zebra_case_data.get('trackedEntities', [])
        if tei_list:
            batch_success = post_individual_teis(zebra_api, tei_list)

        return rj

    finally:
        # If at least some data was sent, run analytics
        if batch_success:
            run_zebra_analytics(zebra_api)


# ----------------------------
# Core Logic Helpers
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

def get_program_attributes(api, program_id):
    params = {'fields': 'programTrackedEntityAttributes[trackedEntityAttribute[id]]'}
    resp = api.get(f'programs/{program_id}', params=params).json()
    return {a['trackedEntityAttribute']['id'] for a in resp.get('programTrackedEntityAttributes', [])}


def get_mapped_ou(source_ou_id, mappings):
    """Translates eIDSR OrgUnit and extracts the clean 11-char UID.
    In the mapping file the mappedID could be a path /uid1/uid2/uid3"""
    ou_map = mappings.get("organisationUnits", {})
    if source_ou_id in ou_map:
        full_mapped_id = ou_map[source_ou_id]["mappedId"]
        return full_mapped_id.split('/')[-1]
    return None


def map_attributes(source_attrs, mappings, allowed_ids=None, log_warnings=False):
    """Maps attributes using Code-to-Code translation for Option Sets."""
    mapped = []
    tea_map = mappings.get("trackedEntityAttributesToTEI", {})
    raw_options = mappings.get("options", {})
    code_lookup = {opt["code"]: opt["mappedCode"] for opt in raw_options.values() if
                   "code" in opt and "mappedCode" in opt}

    for attr in source_attrs:
        src_id = attr.get('attribute')
        val = attr.get('value')

        if allowed_ids and src_id not in allowed_ids:
            continue

        if src_id in tea_map:
            target_id = tea_map[src_id]["mappedId"]
            mapped_val = code_lookup.get(val, val)
            mapped.append({"attribute": target_id, "value": mapped_val})
        elif log_warnings:
            print(f"Warning: TEA ID '{src_id}' is unmapped.")

    return mapped


# ----------------------------
# Main Execution
# ----------------------------

def run_sync(period="today"):
    mappings = load_mappings()
    eidsr_api = Api.from_auth_file(EIDSR_AUTH)
    zebra_api = Api.from_auth_file(ZEBRA_AUTH)

    source_programs = [PROG_EBS, PROG_IBS]
    prog_tea_cache = {pid: get_program_attributes(eidsr_api, pid) for pid in source_programs}

    sync_queue = {}
    now = datetime.utcnow()
    if period == "today":
        start_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')  # Sync last 24h [cite: 5]
    elif period == "this_week":
        # Start of current week (Monday)
        start_date = (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d')
    else:
        start_date = "1900-01-01"  # All time

    for prog_id in source_programs:
        print(f"Sync: Processing {prog_id}...")
        params = {'program': prog_id, 'ouMode': 'ALL', 'enrolledAfter': start_date}

        resp = eidsr_api.get('tracker/enrollments', params=params).json()
        instances = resp.get('instances', resp.get('enrollments', []))
        target_prog_id = mappings["trackerPrograms"][prog_id]["mappedId"]

        for enr in instances:
            tei_id = enr['trackedEntity']
            if tei_id in sync_queue and prog_id == PROG_EBS:
                continue

            tei_params = {'fields': 'trackedEntity,orgUnit,attributes,enrollments[program,enrolledAt,attributes]'}
            tei_full = eidsr_api.get(f'tracker/trackedEntities/{tei_id}', params=tei_params).json()

            allowed_teas = prog_tea_cache[prog_id]
            source_ou = tei_full['orgUnit']
            mapped_ou = get_mapped_ou(source_ou, mappings)

            # If not mapped, verify existence on Zebra directly
            target_ou_to_use = mapped_ou if mapped_ou else source_ou

            if not check_ou_exists_in_zebra(zebra_api, target_ou_to_use):
                print(f"SKIPPING TEI {tei_id}: OrgUnit '{target_ou_to_use}' not found on Zebra server.")
                continue

            mapped_main_attrs = map_attributes(tei_full.get('attributes', []), mappings, allowed_teas,
                                               log_warnings=True)

            target_enr_list = []
            for source_enr in tei_full.get('enrollments', []):
                if source_enr['program'] == prog_id:
                    z_enr_id = None
                    try:
                        z_check_resp = zebra_api.get('tracker/enrollments', params={
                            'trackedEntity': tei_id, 'program': target_prog_id, 'orgUnit': mapped_ou
                        }).json()
                        z_check = z_check_resp.get('instances', z_check_resp.get('enrollments', []))
                        z_enr_id = z_check[0]['enrollment'] if z_check else None
                    except:
                        pass

                    target_enr_list.append({
                        "program": target_prog_id,
                        "enrollment": z_enr_id,
                        "orgUnit": mapped_ou,
                        "status": "ACTIVE",
                        "enrolledAt": source_enr['enrolledAt'],
                        "attributes": map_attributes(source_enr.get('attributes', []), mappings, allowed_teas,
                                                     log_warnings=False)
                    })

            sync_queue[tei_id] = {
                "trackedEntity": tei_id,
                "trackedEntityType": TE_TYPE_ZEBRA,
                "program": target_prog_id,
                "orgUnit": mapped_ou,
                "attributes": mapped_main_attrs,
                "enrollments": target_enr_list
            }

    if sync_queue:
        payload = {'trackedEntities': list(sync_queue.values())}
        with open(PAYLOAD_FILE, 'w') as f:
            json.dump(payload, f, indent=4)
        print(f"Payload saved to {PAYLOAD_FILE}. Executing post...")
        post_data_to_zebra(zebra_api, payload)
    else:
        print("No new data to synchronize.")


if __name__ == "__main__":
    run_sync(period="today")
