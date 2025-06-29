# Optifye Takehome Project

A real-time video processing pipeline that streams RTSP video, processes it through Kafka, and performs inference using machine learning models.

## Architecture Overview

The project consists of several components working together to create a scalable video processing pipeline:

```
optifye-takehome/
├── infra/                      # Infrastructure as Code (Terraform)
│   ├── main.tf                 # Terraform root configuration
│   ├── modules/
│   │   ├── network/            # VPC, subnets, security groups
│   │   ├── kafka/              # MSK cluster or EC2-based Kafka
│   │   ├── eks/                # EKS cluster + node groups
│   │   └── storage/            # S3 bucket for processed videos
│   └── user_data/
│       └── rtsp.sh             # EC2 UserData script for RTSP server
│
├── producer-service/           # RTSP → Kafka producer (Dockerized)
│   ├── main.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .env
│
├── consumer-service/           # Kafka → batch → inference consumer (Dockerized)
│   ├── main.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .env
│
├── postprocess-service/        # Post-processing (Dockerized)
│   ├── main.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .env
│
├── inference/                  # ML model serving (deployed on EKS)
│   ├── app.py                  # Model server (FastAPI/Flask)
│   └── Dockerfile              # Container configuration
│
├── docker-compose.yml          # Compose file for local EC2 Docker deployment
└── README.md
```

## Components

### Infrastructure (`infra/`)
- **Terraform Configuration**: Manages AWS resources including VPC, EKS cluster, MSK, and S3
- **Network Module**: Sets up VPC, subnets, and security groups
- **Kafka Module**: Deploys either MSK cluster or EC2-based Kafka
- **EKS Module**: Kubernetes cluster with node groups
- **Storage Module**: S3 bucket for storing processed videos

### Producer Service (`producer-service/`)
- Connects to RTSP video streams
- Processes video frames
- Publishes frames to Kafka topics
- Runs as a Docker container (managed by Docker Compose)

### Consumer Service (`consumer-service/`)
- Consumes video frames from Kafka
- Batches frames for efficient processing
- Sends batches to inference service (on EKS) via HTTP
- Receives inference results and forwards to post-processing
- Runs as a Docker container (managed by Docker Compose)

### Inference Service (`inference/`)
- ML model server (containerized, deployed on EKS)
- Processes video frame batches
- Returns detection results via HTTP

### Postprocess Service (`postprocess-service/`)
- Draws bounding boxes on detected objects
- Uploads processed images to S3
- Runs as a Docker container (managed by Docker Compose)

## Prerequisites

- AWS CLI configured with appropriate permissions
- Terraform (v1.0+)
- Docker
- kubectl
- Python 3.8+

## Setup Instructions

### 1. Infrastructure Deployment

```bash
cd infra/
terraform init
terraform plan
terraform apply
```

### 2. Build and Run Services with Docker Compose

```bash
# From the project root
cp producer-service/.env.example producer-service/.env   # Edit as needed
cp consumer-service/.env.example consumer-service/.env   # Edit as needed
cp postprocess-service/.env.example postprocess-service/.env   # Edit as needed

# Build and start all services
docker-compose build
docker-compose up -d
```

### 3. Deploy Inference Service on EKS

```bash
# Build and push inference Docker image to ECR
# (see inference/README.md or your own deployment instructions)

# Deploy inference service on EKS using your deployment YAML
kubectl apply -f <your-inference-deployment.yaml>
```

## Usage

1. **Start the pipeline**: Deploy infrastructure and applications
2. **Configure RTSP source**: Update producer-service .env with your RTSP stream URL
3. **Monitor processing**: Check logs and S3 bucket for processed images
4. **Scale as needed**: Adjust EKS node groups and Kafka partitions

## Configuration

### Environment Variables

Each service uses a `.env` file for configuration. Example variables:

- `KAFKA_BROKERS`: Comma-separated list of Kafka broker addresses
- `KAFKA_TOPIC`: Kafka topic for video frames
- `S3_BUCKET`: S3 bucket name for processed images
- `RTSP_URL`: Source RTSP stream URL
- `INFERENCE_SERVICE_URL`: Inference service endpoint (for consumer-service)
- `POST_PROCESSING_SERVICE_URL`: Post-processing service endpoint (for consumer-service)

### Kafka Topics

- `video-frames`: Raw video frames from producer

## Monitoring

- **Docker Compose Logs**: `docker-compose logs -f <service>`
- **EKS Dashboard**: Monitor inference pod health and resource usage
- **Kafka Metrics**: Track message throughput and lag
- **S3**: Monitor processed image uploads


## Troubleshooting

### Common Issues

1. **RTSP Connection Failed**: Check network connectivity and RTSP URL
2. **Kafka Connection Issues**: Verify broker addresses and security groups
3. **Inference Service Unavailable**: Check EKS pod status and logs
4. **S3 Upload Failures**: Verify bucket permissions and IAM roles

### Logs

```bash
# Check Docker Compose logs
cd <service-dir>
docker-compose logs -f <service>

# Check inference service logs on EKS
kubectl logs -f deployment/inference-service

# Check infrastructure logs
terraform logs
```

## Docker Compose Reference

See `docker-compose.yml` for service definitions. Each service loads environment variables from its own `.env` file using the `env_file` directive.

## License

This project is for demonstration purposes as part of the Optifye takehome assignment. 