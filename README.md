steps to connect to vm and start the backend and make it publicly available

**connect to ec2**
ssh -i <your-key.pem> ubuntu@<EC2-PUBLIC-IP>

**install docker and docker compose**

sudo apt update
sudo apt install -y docker.io docker-compose
sudo systemctl enable docker
sudo systemctl start docker

**clone your project**
git clone <your-repo-url>
cd Real-Estate-OpenSearch

**run service with docker compose**
docker-compose up -d

**check opensearch, fastapi containers running or not**
docker ps

**test inside vm**
# OpenSearch cluster info
curl -u admin:pass https://localhost:9200 -k

# Create test index
curl -u admin:pass -X PUT "https://localhost:9200/my-index" -k

# FastAPI docs
curl http://localhost:8000/docs

**install nginx on ec2**
sudo apt install -y nginx
systemctl status nginx

**configure reverse proxy**
sudo nano /etc/nginx/sites-available/realestate

paste
`server {
    listen 80;

    server_name _;

    location /api/ {
        proxy_pass http://127.0.0.1:8000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    location /dashboards/ {
        proxy_pass http://127.0.0.1:5601/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}`

**enable config
sudo ln -s /etc/nginx/sites-available/realestate /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

**access fastapi UI**
http://16.171.146.198:8000/docs

**access opensearch dashboard**

