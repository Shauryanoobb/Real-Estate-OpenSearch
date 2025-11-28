from opensearchpy import OpenSearch

client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "Atharvanoob@1"),  # replace with your creds
    use_ssl=True,
    verify_certs=False
)
