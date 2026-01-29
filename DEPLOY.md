# Deployment Guide: Airflow on GCP VM

This guide deploys Airflow with Playwright for clinic data scraping on a GCP VM.

## Prerequisites

- GCP account with billing enabled
- `gcloud` CLI installed locally
- SSH key configured

## 1. Create GCP VM

```bash
# Set variables
PROJECT_ID="healthcare-ai-agent-481015"
ZONE="asia-southeast1-a"
INSTANCE_NAME="airflow-clinic-sync"

# Create VM (e2-standard-2: 2 vCPU, 8GB RAM)
gcloud compute instances create $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --zone=$ZONE \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --boot-disk-type=pd-balanced \
  --tags=http-server,https-server

# Allow HTTP traffic on port 8080
gcloud compute firewall-rules create allow-airflow \
  --project=$PROJECT_ID \
  --allow=tcp:8080 \
  --target-tags=http-server \
  --description="Allow Airflow webserver"
```

## 2. SSH into VM and Install Docker

```bash
# SSH into VM
gcloud compute ssh $INSTANCE_NAME --zone=$ZONE --project=$PROJECT_ID

# Install Docker
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Add user to docker group (re-login after this)
sudo usermod -aG docker $USER
newgrp docker
```

## 3. Upload Project Files

From your local machine:

```bash
# Create archive of the project
cd /Users/zheyu.lim/Documents/Airflow
tar -czvf airflow-project.tar.gz \
  Dockerfile \
  docker-compose.yml \
  requirements.txt \
  env.template \
  .dockerignore \
  airflow_home/dags \
  airflow_home/credentials

# Upload to VM
gcloud compute scp airflow-project.tar.gz $INSTANCE_NAME:~ --zone=$ZONE --project=$PROJECT_ID
```

## 4. Configure and Start on VM

SSH back into the VM:

```bash
# Extract project
mkdir -p ~/airflow
cd ~/airflow
tar -xzvf ~/airflow-project.tar.gz

# Create directories
mkdir -p airflow_home/logs airflow_home/plugins

# Create .env file from template
cp env.template .env

# Edit .env with your actual values
nano .env
# Update:
#   SUPABASE_URL=https://xuocwhraqvuvqeygacvn.supabase.co
#   SUPABASE_SERVICE_ROLE_KEY=your-actual-key
#   AIRFLOW_UID=$(id -u)

# Set correct permissions
echo "AIRFLOW_UID=$(id -u)" >> .env

# Build and start containers
docker compose build
docker compose up -d

# Check status
docker compose ps
docker compose logs -f airflow-init
```

## 5. Access Airflow UI

1. Get VM external IP:
   ```bash
   gcloud compute instances describe $INSTANCE_NAME --zone=$ZONE --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
   ```

2. Open in browser: `http://<EXTERNAL_IP>:8080`

3. Login:
   - Username: `admin`
   - Password: `admin` (change this!)

## 6. Verify DAGs

In Airflow UI:
- `clinic_facilities_sync` - Daily sheet sync with change detection
- `clinic_enrichment` - Hourly scraping enrichment

Enable both DAGs and trigger a test run.

## 7. Monitoring

```bash
# View logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver

# Check running containers
docker compose ps

# Restart if needed
docker compose restart

# Stop all
docker compose down

# Start all
docker compose up -d
```

## 8. Security Recommendations

1. **Change default password**: Go to Airflow UI → Security → Users
2. **Restrict firewall**: Limit port 8080 to your IP only
3. **Use HTTPS**: Add nginx reverse proxy with Let's Encrypt
4. **Rotate credentials**: Update Supabase/Google keys periodically

## Troubleshooting

### Playwright/Chromium issues
```bash
# Check if Playwright is working
docker compose exec airflow-scheduler python -c "from playwright.sync_api import sync_playwright; print('OK')"
```

### DAG import errors
```bash
docker compose exec airflow-scheduler airflow dags list
```

### Database issues
```bash
docker compose exec airflow-scheduler airflow db check
```

### Reset everything
```bash
docker compose down -v  # Removes volumes too
docker compose up -d
```



