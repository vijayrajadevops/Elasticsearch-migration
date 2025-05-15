#!/usr/bin/env python3

import subprocess
import time
import signal
import sys
import boto3
import os
import json
import requests

#AWS OpenSearch secret
ELASTICSEARCH_URL = ""  #opensearch connection string
os_keys_to_extract =[ "ELASTICSEARCH_URL"]


#Secret-manager
secret_name="database-migration/rc/db-secrets"  #RC
os_secret_name="es/es-klara-rc-shared-us-1/connection_string"
region_name="us-east-1"

# List of indices to delete (Modify this list as needed)
indices_to_delete = ["patient-profiles-performance","epic-patient-identities"]  #Allowed values patient-profiles-performance,epic-patient-identities

# Function to delete an index
# Function to delete an index
def delete_index(index_name):
    url = f"{ELASTICSEARCH_URL}/{index_name}"

    response = requests.delete(url)  # No authentication needed
    if response.status_code == 200:
        print(f"✅ Deleted: {index_name}")
    elif response.status_code == 404:
        print(f"⚠️ Not Found: {index_name}")
    else:
        print(f"❌ Failed to delete {index_name} - {response.status_code}: {response.text}")


#Retrieve secrets from secretmanager
def get_secret(secret_name, region_name):
    """Retrieve a secret from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        else:
            return json.loads(base64.b64decode(response["SecretBinary"]).decode("utf-8"))
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

#extract secret values
def extract_secrets(secrets, keys):
    """Extract required secrets and store them as global variables."""
    if secrets:
        for key in keys:
            globals()[key] = secrets.get(key, "")
        return True
    else:
        print("Failed to retrieve secrets.")
        return False

# === MAIN FUNCTION ===
def main():


    #Secret extraction starts
    os_secrets=get_secret(os_secret_name,region_name)
    os_success = extract_secrets(os_secrets, os_keys_to_extract)

    if os_success:
        print("✅ Secrets loaded successfully!\n")
    #secret extraction ends

    try:
        time.sleep(5)
        # Loop through the list and delete indices
        for index in indices_to_delete:
            delete_index(index)

        print(f"🎯 Index {index} deletion process completed!")

    except Exception as e:
        print(f"[❌] Error occurred: {e}")

    finally:
        print("[ℹ️] finally ")
        # tunnel_process.terminate()

# === EXECUTE SCRIPT ===
if __name__ == "__main__":
    main()
