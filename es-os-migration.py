#!/usr/bin/env python3

import subprocess
import time
import datetime  
import sys
import boto3
import os
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

#config
SCROLL_TIMEOUT = "5m"  # Longer scroll timeout
BATCH_SIZE = 10000  # Fetch more documents per request
MAX_RETRIES = 5  # Number of retries for failures
MAX_WORKERS = 2 # For parallel execution (tune based on CPU load)

ES_DB_HANDLE = None
USERNAME = None
ES_PASSWORD = None
ES_HOST = None
ENV = None
ELASTICSEARCH_URL = None
indices_to_migrate= None

# Configure logging
LOG_FILE = "/var/log/rc_migration.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# === FETCH INDEX SETTINGS & MAPPINGS ===
def get_index_metadata(index_name):
    print(f"[+] Fetching settings & mappings for index: {index_name}")

    settings_cmd = f'curl -s "{ES_HOST}/{index_name}/_settings"'
    mappings_cmd = f'curl -s "{ES_HOST}/{index_name}/_mapping"'

    settings_result = subprocess.run(settings_cmd, shell=True, capture_output=True, text=True)
    mappings_result = subprocess.run(mappings_cmd, shell=True, capture_output=True, text=True)

    if settings_result.returncode == 0 and mappings_result.returncode == 0:
        settings = json.loads(settings_result.stdout)
        mappings = json.loads(mappings_result.stdout)

        index_settings = settings.get(index_name, {}).get("settings", {}).get("index", {})
        index_mappings = mappings.get(index_name, {}).get("mappings", {})

        # **Print the fetched metadata**
        print(f"\n[📄] Index: {index_name}")
        # print(f"[⚙️] Settings: {json.dumps(index_settings, indent=2)}")
        # print(f"[📊] Mappings: {json.dumps(index_mappings, indent=2)}\n")

        return {
            "index": index_name,
            "settings": index_settings,
            "mappings": index_mappings
        }
    else:
        print(f"[❌] Failed to fetch metadata for {index_name}: {settings_result.stderr} {mappings_result.stderr}")
        return None


# === CREATE INDEX IN OPENSEARCH ===
def create_index_in_os(index_name, metadata):
    print(f"[+] Cleaning metadata for index '{index_name}'...")

    # Clean metadata before sending the request
    cleaned_metadata = clean_index_metadata(metadata)

    # Check if the index already exists
    check_url = f"{ELASTICSEARCH_URL}/{index_name}"
    check_response = requests.head(check_url)  # HEAD request is lighter than GET

    if check_response.status_code == 200:
        print(f"[ℹ️] Index '{index_name}' already exists. Skipping creation.")
        return  # Skip creation

    print(f"[+] Creating index '{index_name}' in OpenSearch...")

    url = f"{ELASTICSEARCH_URL}/{index_name}"
    headers = {"Content-Type": "application/json"}

    response = requests.put(url, headers=headers, json=cleaned_metadata)

    if response.status_code in [200, 201]:
        print(f"[✅] Index '{index_name}' created successfully in OpenSearch.")
    else:
        print(f"[❌] Failed to create index '{index_name}' in OpenSearch: {response.text}")

def migrate_index(index_name):
    logging.info(f"[🔄] Starting migration for {index_name}")

    try:
        session = requests.Session()
        scroll_url = f"{ES_HOST}/{index_name}/_search?scroll={SCROLL_TIMEOUT}"
        headers = {"Content-Type": "application/json"}
        query = {"size": BATCH_SIZE, "query": {"match_all": {}}}

        # 🔹 Log ES command once
        logging.info(f"[🔍] Sample ES Command: POST {scroll_url}\nPayload: {json.dumps(query, indent=2)}")

        response = session.post(scroll_url, headers=headers, json=query)
        if response.status_code != 200:
            logging.error(f"[❌] Failed to start scroll for {index_name}: {response.text}")
            return

        scroll_id = response.json().get("_scroll_id")
        total_docs = response.json().get("hits", {}).get("total", {}).get("value", 0) or 0
        migrated_docs = 0
        iteration_count = 0  # Track iterations

        logging.info(f"[📊] {index_name}: Found {total_docs} documents to migrate.")

        printed_os_command = False  # Flag to log OS command once

        while scroll_id:
            hits = response.json().get("hits", {}).get("hits", [])
            if not hits:
                logging.info(f"[✅] No more documents to migrate for {index_name}.")
                break

            # Prepare bulk insert payload
            bulk_payload = "\n".join(
                json.dumps({"index": {"_index": index_name, "_id": doc["_id"]}}) + "\n" + json.dumps(doc["_source"])
                for doc in hits
            ) + "\n"

            bulk_url = f"{ELASTICSEARCH_URL}/_bulk?refresh=false"

            # 🔹 Log OS command only once
            if not printed_os_command:
                logging.info(f"[🔍] Sample OS Command: POST {bulk_url}\nPayload (first 2 docs):\n{bulk_payload[:500]}...")
                printed_os_command = True  # Set flag to True

            response = session.post(bulk_url, headers=headers, data=bulk_payload)

            if response.status_code == 200:
                migrated_docs += len(hits)
                iteration_count += 1  # Increment iteration count

                # 🔹 Log progress every 5 batches
                if iteration_count % 5 == 0:
                    logging.info(f"[⏳] Progress: {migrated_docs}/{total_docs} documents migrated.")

            else:
                logging.error(f"[❌] [{index_name}] Failed to insert batch: {response.text}")
                return

            # Get next batch
            response = session.post(f"{ES_HOST}/_search/scroll", headers=headers, json={"scroll": SCROLL_TIMEOUT, "scroll_id": scroll_id})
            if response.status_code != 200:
                logging.error(f"[❌] Failed to fetch next batch for {index_name}: {response.text}")
                break

            scroll_id = response.json().get("_scroll_id")
            if not scroll_id:
                logging.warning("[⚠️] Scroll ID missing, stopping migration.")
                break

        # 🔄 Final refresh after migration is completed
        final_refresh_url = f"{ELASTICSEARCH_URL}/{index_name}/_refresh"
        logging.info(f"[🔄] Final refresh for index {index_name} -> POST {final_refresh_url}")
        final_refresh_response = session.post(final_refresh_url, headers=headers)

        if final_refresh_response.status_code == 200:
            logging.info(f"[✅] Final refresh completed for {index_name}")
        else:
            logging.warning(f"[⚠️] Failed to perform final refresh for {index_name}: {final_refresh_response.text}")

        logging.info(f"[✅] Migration completed for {index_name}. Total migrated: {migrated_docs}/{total_docs}")

    except Exception as e:
        logging.exception(f"[❌] Unexpected error in migration of {index_name}: {e}")

#==========HELPERS=========


def extract_keys_from_json(json_path, keys_to_extract):
    with open(json_path, 'r') as f:
        data = json.load(f)

    extracted = {}
    
    for key in keys_to_extract :
        if key in data:
            extracted[key] = data[key]
        else:
            extracted[key] = None  # or raise an error if key is mandatory

    return extracted

def set_globals(config):
    global ES_DB_HANDLE, USERNAME, ES_PASSWORD, ES_HOST, ENV, ELASTICSEARCH_URL

    ES_DB_HANDLE = config["ES_DB_HANDLE"]
    USERNAME = config["USERNAME"]
    ES_PASSWORD = config["ES_PASSWORD"]
    ES_HOST = config["ES_HOST"]
    ENV = config["ENV"]
    ELASTICSEARCH_URL = config["ELASTICSEARCH_URL"]

#clean metdata
def clean_index_metadata(metadata):
    """Remove system-generated settings that are not allowed in index creation."""
    forbidden_keys = ["creation_date", "provided_name", "uuid"]  # 🔹 Removed "index.version.created" from here

    # Clean top-level settings
    cleaned_settings = {k: v for k, v in metadata["settings"].items() if k not in forbidden_keys}

    # 🔹 Check if "version" exists inside settings and is a dictionary
    if "version" in cleaned_settings and isinstance(cleaned_settings["version"], dict):
        cleaned_settings["version"] = {k: v for k, v in cleaned_settings["version"].items() if k != "created"}

    cleaned_metadata = {
        "settings": cleaned_settings,
        "mappings": metadata["mappings"]
    }

    return cleaned_metadata

# get indexes
def get_source_indices():
    """ Fetch indices from the source Elasticsearch. """
    response = requests.get(f"{ES_HOST}/_cat/indices?format=json")

    if response.status_code != 200:
        print(f"[❌] Failed to fetch indices: {response.text}")
        return []

    return [index["index"] for index in response.json()]

def get_source_indices_names():
    print("[+] Fetching Elasticsearch indexes...")
    url = f"{ES_HOST}/_cat/indices?v&format=json"
    response = requests.get(url, auth=(USERNAME, ES_PASSWORD))

    if response.status_code == 200:
        indexes = response.json()
        index_names = [idx["index"] for idx in indexes]  # Extract only index names

        print(f"[✅] Found {len(index_names)} indexes.")
        print(index_names)  # Print only index names
        return index_names
    else:
        print("[❌] Failed to fetch indexes:", response.text)
        sys.exit(1)

# === MAIN FUNCTION ===
def main():


    #Secret keys
    keys_to_extract = ["ES_DB_HANDLE","USERNAME","ES_PASSWORD","ES_HOST","ENV","ELASTICSEARCH_URL","INDICES_TO_MIGRATE"]

    config = extract_keys_from_json('config.json', keys_to_extract)
    set_globals(config)
    indices_to_migrate = config.get("INDICES_TO_MIGRATE", [])
    print("Indices to migrate:", indices_to_migrate)

    try:
        time.sleep(5)
        indexes_names = get_source_indices_names()
        indexes=get_source_indices()


        #extract settings and mappings from Aptible-ES and migrate to Opensearch
        for index in indexes_names:
            metadata = get_index_metadata(index)  # Extract settings & mappings
            if metadata:
                create_index_in_os(index, metadata)  # Create index in OpenSearch
            else:
                print(f"[⚠️] No metadata found for index '{index}'")
            
        start_time = time.time()  # Record start time

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(migrate_index, index): index for index in indices_to_migrate}
            for future in as_completed(futures):
                try:
                    result = future.result()  # This will raise any exceptions
                    print(f"[✅] Successfully migrated index: {futures[future]}")
                except Exception as e:
                    print(f"[❌] Error migrating {futures[future]}: {e}")
        end_time = time.time()  # Record end time
        execution_time = end_time - start_time
        
        # Convert execution time to a readable format
        if execution_time < 60:
            time_str = f"{execution_time:.2f} seconds"
        elif execution_time < 3600:
            time_str = f"{execution_time / 60:.2f} minutes"
        else:
            time_str = f"{execution_time / 3600:.2f} hours"

        logging.info(f"Migration ended at: {datetime.datetime.fromtimestamp(end_time)}")
        logging.info(f"Total execution time: {time_str}")
            
    except Exception as e:
        print(f"[❌] Error occurred: {e}")

    finally:
        print("[ℹ️] Migration script ends  ") 

# === EXECUTE SCRIPT ===
if __name__ == "__main__":
    main()

