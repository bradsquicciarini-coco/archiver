VPC_ID=vpc-0a3eb62e4444089a7

SUBNET_A_ID=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --availability-zone us-west-2a \
  --cidr-block 10.10.64.0/20 \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=coco-eks-private-2a-10.10.64.0-20}]' \
  --query 'Subnet.SubnetId' --output text)

SUBNET_B_ID=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --availability-zone us-west-2b \
  --cidr-block 10.10.80.0/20 \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=coco-eks-private-2b-10.10.80.0-20}]' \
  --query 'Subnet.SubnetId' --output text)

SUBNET_C_ID=$(aws ec2 create-subnet \
  --vpc-id $VPC_ID \
  --availability-zone us-west-2c \
  --cidr-block 10.10.96.0/20 \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=coco-eks-private-2c-10.10.96.0-20}]' \
  --query 'Subnet.SubnetId' --output text)

echo "Created: $SUBNET_A_ID $SUBNET_B_ID $SUBNET_C_ID"
