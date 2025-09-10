import os
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import requests

# env vars
SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
SEARCH_API_KEY = os.getenv("AZURE_SEARCH_ADMIN_KEY")
STORAGE_CONN_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINERS = os.getenv("BLOB_CONTAINERS", "")

HEADERS = {"Content-Type": "application/json", "api-key": SEARCH_API_KEY}

def log(msg: str):
    print(f"[INDEXER] {msg}", flush=True)

def get_target_containers():
    if not BLOB_CONTAINERS:
        raise ValueError("BLOB_CONTAINERS env var is not set.")
    target = [c.strip() for c in BLOB_CONTAINERS.split(",")]
    blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
    available = [c.name for c in blob_service.list_containers()]
    log(f"Available containers in storage: {available}")
    valid = [c for c in target if c in available]
    if not valid:
        raise ValueError("No valid containers found to index.")
    log(f"Target containers to index: {valid}")
    return valid

def create_search_index():
    log("Creating or validating search index: financial-index")
    url = f"{SEARCH_ENDPOINT}/indexes?api-version=2024-07-01"
    payload = {
        "name": "financial-index",
        "fields": [
            {"name": "id", "type": "Edm.String", "key": True, "searchable": False},
            {"name": "content", "type": "Edm.String", "searchable": True},
            {"name": "metadata_storage_name", "type": "Edm.String", "filterable": True, "sortable": True},
            {"name": "metadata_storage_path", "type": "Edm.String", "filterable": True},
            {"name": "metadata_storage_last_modified", "type": "Edm.DateTimeOffset", "sortable": True}
        ]
    }
    response = requests.post(url, headers=HEADERS, json=payload)
    log(f"Index creation response: {response.status_code} - {response.text}")

def create_data_source(container_name):
    log(f"Creating datasource for container: {container_name}")
    url = f"{SEARCH_ENDPOINT}/datasources/{container_name}-ds?api-version=2024-07-01"
    payload = {
        "name": f"{container_name}-ds",
        "type": "azureblob",
        "credentials": {"connectionString": STORAGE_CONN_STRING},
        "container": {"name": container_name}
    }
    response = requests.put(url, headers=HEADERS, json=payload)
    log(f"Datasource response: {response.status_code} - {response.text}")

def create_indexer(container_name):
    log(f"Creating indexer for container: {container_name}")
    url = f"{SEARCH_ENDPOINT}/indexers/{container_name}-idx?api-version=2024-07-01"
    payload = {
        "name": f"{container_name}-idx",
        "dataSourceName": f"{container_name}-ds",
        "targetIndexName": "financial-index",
        "parameters": {
            "maxFailedItems": -1,
            "maxFailedItemsPerBatch": -1,
            "configuration": {
                "failOnUnsupportedContentType": False,
                "failOnUnprocessableDocument": False,
                "indexedFileNameExtensions": ".pdf,.docx,.xlsx,.md,.txt",
                "excludedFileNameExtensions": ".png,.jpeg",
                "dataToExtract": "contentAndMetadata"
            }
        }
    }
    response = requests.put(url, headers=HEADERS, json=payload)
    log(f"Indexer response: {response.status_code} - {response.text}")

def run_indexing():
    log("Starting indexing workflow")
    create_search_index()
    containers = get_target_containers()
    for c in containers:
        create_data_source(c)
        create_indexer(c)
    log("Indexing complete.")
    return {"status": "success", "containers": containers}

def manual_index(req: func.HttpRequest) -> func.HttpResponse:
    log("HTTP trigger invoked")
    try:
        result = run_indexing()
        return func.HttpResponse(json.dumps(result), status_code=200, mimetype="application/json")
    except Exception as e:
        log(f"Error: {str(e)}")
        return func.HttpResponse(json.dumps({"status": "error", "message": str(e)}), status_code=500, mimetype="application/json")
