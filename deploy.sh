#!/usr/bin/env bash
set -euo pipefail

echo "=== Starting OpenSky ELT Infrastructure Deployment ==="

# 1. Update system packages
echo "--> Updating system packages..."
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release git wget

# 2. Install Docker & Docker Compose Plugin if not installed
if ! command -v docker &> /dev/null; then
    echo "--> Installing Docker..."
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg --yes
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update -y
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    sudo usermod -aG docker "$USER"
    echo "Docker installed successfully."
else
    echo "Docker is already installed."
fi

# 3. Install Terraform if not installed
if ! command -v terraform &> /dev/null; then
    echo "--> Installing Terraform..."
    wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com/ /usr/share/keyrings/hashicorp-archive-keyring.gpg $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
    sudo apt-get update -y && sudo apt-get install -y terraform
    echo "Terraform installed successfully."
else
    echo "Terraform is already installed."
fi

# 4. Clone or Pull Repository if REPO_URL is set
REPO_URL="${REPO_URL:-}"
if [ -n "$REPO_URL" ]; then
    PROJECT_DIR="opensky-elt-pipeline"
    if [ ! -d "$PROJECT_DIR" ]; then
        echo "--> Cloning repository from $REPO_URL..."
        git clone "$REPO_URL" "$PROJECT_DIR"
        cd "$PROJECT_DIR"
    else
        echo "--> Updating repository in $PROJECT_DIR..."
        cd "$PROJECT_DIR"
        git pull origin main
    fi
fi

# 5. Check for .env file and export variables
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        echo "--> Creating .env from .env.example..."
        cp .env.example .env
        echo "WARNING: Please update .env with your actual GCP credentials and settings before proceeding."
    else
        echo "ERROR: .env file missing and .env.example not found!"
        exit 1
    fi
fi

set -a
source .env
set +a

# 6. Provision Infrastructure with Terraform
if [ -d "terraform" ]; then
    echo "--> Provisioning GCP infrastructure with Terraform..."
    cd terraform
    terraform init
    terraform apply -auto-approve \
      -var="project_id=${GCP_PROJECT_ID}" \
      -var="gcs_bucket_name=${GCS_BUCKET_NAME}" \
      -var="region=${GCP_REGION:-us-central1}"
    cd ..
fi

# 7. Build & Start Services via Docker Compose
echo "--> Starting containers with Docker Compose..."
docker compose up -d --build

echo "=== Deployment Complete! Services are running: ==="
docker compose ps
