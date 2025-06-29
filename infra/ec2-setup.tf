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

# EC2 Instance
resource "aws_instance" "pipeline_server" {
  ami                    = "ami-0f5ee92e2d63afc18"  # Ubuntu 22.04 LTS in ap-south-1
  instance_type          = "t2.micro"
  key_name              = "optifye-key"
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
    Name = "optifye-pipeline-server"
  }
}

# Output the public IP
output "public_ip" {
  value = aws_instance.pipeline_server.public_ip
}

output "instance_id" {
  value = aws_instance.pipeline_server.id
} 