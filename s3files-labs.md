# S3 Files Lab - Mount S3 Bucket as File System on EC2

## Prerequisites
- AWS Account with S3 Files available in your region
- S3 bucket created
- EC2 key pair for SSH access
- S3 Files file system created via AWS Console

## Lab Overview
This lab shows how to mount an S3 bucket as a file system on EC2 using S3 Files or Mountpoint for S3.

---

## Lab runbook: EC2, SSH, and S3 Files mount

Use this block when your S3 Files file system is already created and you only need an EC2 instance, instance ID, and SSH.

**Set your values (replace every placeholder):**

| Item | Placeholder |
|------|-------------|
| File system ID | `YOUR_FILE_SYSTEM_ID` |
| S3 bucket | `YOUR_BUCKET_NAME` |
| Region | `YOUR_REGION` |
| AWS CLI profile | `YOUR_AWS_PROFILE` |
| EC2 key pair name (in that region) | `YOUR_KEY_PAIR` |
| Instance name tag (optional) | `YOUR_INSTANCE_NAME_TAG` |

**Prerequisites:** IAM instance profile `S3FilesEC2Profile` (from Step 2 below) attached to a role with `AmazonS3FilesClientFullAccess` and S3 access to `YOUR_BUCKET_NAME`. EC2 key pair `YOUR_KEY_PAIR` exists in `YOUR_REGION`. Default VPC has a public subnet if you want a public IP for SSH.

### 1) Resolve default VPC, subnet, and default security group

```bash
export AWS_PROFILE=YOUR_AWS_PROFILE
export AWS_REGION=YOUR_REGION
export AWS_DEFAULT_REGION="$AWS_REGION"

VPC_ID=$(aws ec2 describe-vpcs \
  --region "$AWS_REGION" \
  --filters "Name=isDefault,Values=true" \
  --query 'Vpcs[0].VpcId' --output text)

SUBNET_ID=$(aws ec2 describe-subnets \
  --region "$AWS_REGION" \
  --filters "Name=vpc-id,Values=$VPC_ID" \
  --query 'Subnets[0].SubnetId' --output text)

SG_ID=$(aws ec2 describe-security-groups \
  --region "$AWS_REGION" \
  --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=default" \
  --query 'SecurityGroups[0].GroupId' --output text)

echo "VPC=$VPC_ID SUBNET=$SUBNET_ID SG=$SG_ID"
```

Replace `YOUR_KEY_PAIR` with your key pair name in `YOUR_REGION` (it must match the key you use with `YOUR_KEY.pem` below).

### 2) Launch EC2 and print instance ID

```bash
export INSTANCE_NAME_TAG=YOUR_INSTANCE_NAME_TAG

INSTANCE_ID=$(aws ec2 run-instances \
  --region "$AWS_REGION" \
  --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --instance-type t3.medium \
  --key-name YOUR_KEY_PAIR \
  --security-group-ids "$SG_ID" \
  --subnet-id "$SUBNET_ID" \
  --iam-instance-profile Name=S3FilesEC2Profile \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${INSTANCE_NAME_TAG}}]" \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "InstanceId: $INSTANCE_ID"
```

**Output to share:** the printed `InstanceId` value (for example `i-0abc123def4567890`).

### 3) Wait until running, then get public IP (if assigned)

```bash
aws ec2 wait instance-running --region "$AWS_REGION" --instance-ids "$INSTANCE_ID"

PUBLIC_IP=$(aws ec2 describe-instances \
  --region "$AWS_REGION" \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text)

echo "Public IP: $PUBLIC_IP"
```

If `Public IP` is `None`, the subnet may not auto-assign public IPs; either enable it on the subnet, attach an Elastic IP, or use Session Manager / a bastion instead of SSH.

### 4) SSH into the new instance

```bash
ssh -i /path/to/YOUR_KEY.pem ec2-user@"$PUBLIC_IP"
```

### 5) Mount S3 Files on the instance (after efs-utils / S3 Files client install)

```bash
export FILE_SYSTEM_ID=YOUR_FILE_SYSTEM_ID
sudo mkdir -p /mnt/s3files
sudo mount -t s3files "$FILE_SYSTEM_ID" /mnt/s3files
```

---

## Step 1: Prepare S3 Bucket

```bash
export AWS_PROFILE=YOUR_AWS_PROFILE
export AWS_REGION=YOUR_REGION
export AWS_DEFAULT_REGION="$AWS_REGION"

# Enable versioning (required for S3 Files)
aws s3api put-bucket-versioning \
  --bucket YOUR_BUCKET_NAME \
  --region "$AWS_REGION" \
  --versioning-configuration Status=Enabled
```

---

## Step 2: Create IAM Role for EC2

### Create the role with trust policy
```bash
aws iam create-role \
  --role-name S3FilesEC2Role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "ec2.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'
```

### Attach AWS managed policies
```bash
# For S3 Files access
aws iam attach-role-policy \
  --role-name S3FilesEC2Role \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FilesClientFullAccess

# For CloudWatch monitoring (optional)
aws iam attach-role-policy \
  --role-name S3FilesEC2Role \
  --policy-arn arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy
```

### Add inline policy for S3 bucket access
```bash
aws iam put-role-policy \
  --role-name S3FilesEC2Role \
  --policy-name S3BucketAccess \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        "Resource": [
          "arn:aws:s3:::YOUR_BUCKET_NAME",
          "arn:aws:s3:::YOUR_BUCKET_NAME/*"
        ]
      },
      {
        "Effect": "Allow",
        "Action": "s3:ListAllMyBuckets",
        "Resource": "*"
      }
    ]
  }'
```

### Create instance profile
```bash
aws iam create-instance-profile \
  --instance-profile-name S3FilesEC2Profile

aws iam add-role-to-instance-profile \
  --instance-profile-name S3FilesEC2Profile \
  --role-name S3FilesEC2Role
```

---

## Step 3: Create VPC Endpoints (if not exist)

### Check if S3 Files endpoint exists
```bash
aws ec2 describe-vpc-endpoints \
  --region YOUR_REGION \
  --filters "Name=vpc-id,Values=YOUR_VPC_ID" \
  --query 'VpcEndpoints[?ServiceName==`aws.api.YOUR_REGION.s3files`]'
```

### Create S3 Files Interface Endpoint (if needed)
```bash
aws ec2 create-vpc-endpoint \
  --region YOUR_REGION \
  --vpc-id YOUR_VPC_ID \
  --service-name aws.api.YOUR_REGION.s3files \
  --vpc-endpoint-type Interface \
  --subnet-ids YOUR_SUBNET_ID \
  --security-group-ids YOUR_SECURITY_GROUP_ID
```

### Create S3 Gateway Endpoint (if needed)
```bash
aws ec2 create-vpc-endpoint \
  --region YOUR_REGION \
  --vpc-id YOUR_VPC_ID \
  --service-name com.amazonaws.YOUR_REGION.s3 \
  --route-table-ids YOUR_ROUTE_TABLE_ID
```

---

## Step 4: Configure Security Group

### Add required rules to your security group
```bash
# Allow SSH
aws ec2 authorize-security-group-ingress \
  --region YOUR_REGION \
  --group-id YOUR_SECURITY_GROUP_ID \
  --protocol tcp \
  --port 22 \
  --cidr 0.0.0.0/0

# Allow HTTPS (for VPC endpoint)
aws ec2 authorize-security-group-ingress \
  --region YOUR_REGION \
  --group-id YOUR_SECURITY_GROUP_ID \
  --protocol tcp \
  --port 443 \
  --source-group YOUR_SECURITY_GROUP_ID

# Allow NFS (for S3 Files)
aws ec2 authorize-security-group-ingress \
  --region YOUR_REGION \
  --group-id YOUR_SECURITY_GROUP_ID \
  --protocol tcp \
  --port 2049 \
  --source-group YOUR_SECURITY_GROUP_ID
```

---

## Step 5: Launch EC2 Instance

```bash
export AWS_PROFILE=YOUR_AWS_PROFILE
export AWS_REGION=YOUR_REGION
export AWS_DEFAULT_REGION="$AWS_REGION"
export INSTANCE_NAME_TAG=YOUR_INSTANCE_NAME_TAG

aws ec2 run-instances \
  --region "$AWS_REGION" \
  --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --instance-type t3.medium \
  --key-name YOUR_KEY_PAIR \
  --security-group-ids YOUR_SECURITY_GROUP_ID \
  --subnet-id YOUR_SUBNET_ID \
  --iam-instance-profile Name=S3FilesEC2Profile \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${INSTANCE_NAME_TAG}}]"
```

---

## Step 6: Mount S3 Bucket on EC2

### SSH to your EC2 instance
```bash
ssh -i YOUR_KEY.pem ec2-user@YOUR_EC2_PUBLIC_IP
```

### Option A: Using S3 Files (if client available)
```bash
# Install S3 Files utilities
sudo yum install -y amazon-efs-utils

# Create mount point
sudo mkdir -p /mnt/s3files

# Mount using S3 Files file system ID
sudo mount -t s3files YOUR_FILE_SYSTEM_ID /mnt/s3files

# Verify
df -h | grep s3files
```

### Option B: Using Mountpoint for S3 (Recommended)
```bash
# Download and install Mountpoint for S3
wget https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm
sudo yum install -y ./mount-s3.rpm

# Create mount point
sudo mkdir -p /mnt/s3bucket

# Mount your S3 bucket
sudo mount-s3 YOUR_BUCKET_NAME /mnt/s3bucket

# Verify
df -h | grep s3
```

---

## Step 7: Test File Operations

```bash
# List files
ls -la /mnt/s3bucket/

# Create a test file
echo "Hello from S3 Files" | sudo tee /mnt/s3bucket/test.txt

# Read the file
cat /mnt/s3bucket/test.txt

# Verify in S3
aws s3 ls s3://YOUR_BUCKET_NAME/
```

---

## Step 8: Make Mount Persistent (Optional)

### For Mountpoint for S3
```bash
echo "YOUR_BUCKET_NAME /mnt/s3bucket fuse.mount-s3 _netdev,allow-delete,allow-other,region=YOUR_REGION 0 0" | sudo tee -a /etc/fstab
```

### For S3 Files
```bash
echo "YOUR_FILE_SYSTEM_ID /mnt/s3files s3files defaults,_netdev 0 0" | sudo tee -a /etc/fstab
```

---

## Quick Reference

### Required IAM Permissions
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`
- `s3:ListBucket`, `s3:ListAllMyBuckets`
- `s3files:MountFileSystem` (for S3 Files)

### Required Network Configuration
- VPC Interface Endpoint: `aws.api.YOUR_REGION.s3files`
- VPC Gateway Endpoint: `com.amazonaws.YOUR_REGION.s3`
- Security Group Ports: 22 (SSH), 443 (HTTPS), 2049 (NFS)

### S3 Bucket Requirements
- Versioning: Enabled
- Encryption: SSE-S3 or SSE-KMS

---

## Troubleshooting

### Mount timeout
```bash
# Check VPC endpoint status
aws ec2 describe-vpc-endpoints --region YOUR_REGION --vpc-endpoint-ids YOUR_ENDPOINT_ID

# Check security group rules
aws ec2 describe-security-groups --region YOUR_REGION --group-ids YOUR_SECURITY_GROUP_ID

# View mount logs
sudo dmesg | tail -30
```

### Permission denied
```bash
# Verify IAM role is attached
aws ec2 describe-instances --instance-ids YOUR_INSTANCE_ID --query 'Reservations[0].Instances[0].IamInstanceProfile'

# Check bucket versioning
aws s3api get-bucket-versioning --bucket YOUR_BUCKET_NAME
```

---

## Cleanup

```bash
# Unmount
sudo umount /mnt/s3bucket

# Terminate EC2
aws ec2 terminate-instances --instance-ids YOUR_INSTANCE_ID

# Delete VPC endpoints
aws ec2 delete-vpc-endpoints --vpc-endpoint-ids YOUR_ENDPOINT_ID

# Delete IAM resources
aws iam remove-role-from-instance-profile --instance-profile-name S3FilesEC2Profile --role-name S3FilesEC2Role
aws iam delete-instance-profile --instance-profile-name S3FilesEC2Profile
aws iam detach-role-policy --role-name S3FilesEC2Role --policy-arn arn:aws:iam::aws:policy/AmazonS3FilesClientFullAccess
aws iam delete-role-policy --role-name S3FilesEC2Role --policy-name S3BucketAccess
aws iam delete-role --role-name S3FilesEC2Role
```

---

## Summary

This lab demonstrated:
1. Creating IAM role with S3 Files permissions
2. Setting up VPC endpoints for S3 Files
3. Launching EC2 with proper IAM role
4. Mounting S3 bucket as file system using Mountpoint for S3
5. Performing standard file operations on S3 data

**Key Benefit**: Access S3 data using standard file system operations without changing application code!