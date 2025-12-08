from opensearchpy import OpenSearch
import os

# OpenSearch host: uses environment variable or defaults to Docker service name
# For local development outside Docker, set OPENSEARCH_HOST="localhost"
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "opensearch-node1")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))

client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
    http_auth=("admin", "Atharvanoob@1"),  # replace with your creds
    use_ssl=True,
    verify_certs=False
)
