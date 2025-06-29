terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1"
}

# Key Pair for EC2 instances
resource "aws_key_pair" "optifye_key" {
  key_name   = "optifye-key"
  public_key = file("~/.ssh/id_rsa.pub")  # Use your existing SSH public key
}

# Security Group for EC2 instance
resource "aws_security_group" "pipeline_sg" {
  name        = "optifye-pipeline-sg"
  description = "Security group for Optifye video processing pipeline"

  ingress {
    description = "SSH from anywhere"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "RTSP server"
    from_port   = 8554
    to_port     = 8554
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Kafka port"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTP for web services"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS for web services"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "optifye-pipeline-sg"
  }
}

# EC2 Instance for Kafka/Zookeeper (existing)
resource "aws_instance" "pipeline_server" {
  ami                    = "ami-0f5ee92e2d63afc18"  # Ubuntu 22.04 LTS in ap-south-1
  instance_type          = "t2.micro"
  key_name              = aws_key_pair.optifye_key.key_name
  vpc_security_group_ids = [aws_security_group.pipeline_sg.id]

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = <<-EOF
              #!/bin/bash
              apt-get update
              apt-get install -y docker.io docker-compose
              systemctl start docker
              systemctl enable docker
              usermod -aG docker ubuntu
              EOF

  tags = {
    Name = "optifye-kafka-server"
  }
}

# EC2 Instance for Producer, Consumer, and Post-processing services
resource "aws_instance" "pipeline_services" {
  ami                    = "ami-0f5ee92e2d63afc18"  # Ubuntu 22.04 LTS in ap-south-1
  instance_type          = "t2.medium"
  key_name              = aws_key_pair.optifye_key.key_name
  vpc_security_group_ids = [aws_security_group.pipeline_sg.id]

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  user_data = <<-EOF
              #!/bin/bash
              apt-get update
              apt-get install -y docker.io docker-compose git
              systemctl start docker
              systemctl enable docker
              usermod -aG docker ubuntu
              
              # Install additional dependencies for video processing
              apt-get install -y python3-pip python3-venv
              EOF

  tags = {
    Name = "optifye-pipeline-services"
  }
}

# Output the public IPs
output "kafka_server_ip" {
  value = aws_instance.pipeline_server.public_ip
}

output "pipeline_services_ip" {
  value = aws_instance.pipeline_services.public_ip
}

output "kafka_instance_id" {
  value = aws_instance.pipeline_server.id
}

output "pipeline_services_instance_id" {
  value = aws_instance.pipeline_services.id
} 