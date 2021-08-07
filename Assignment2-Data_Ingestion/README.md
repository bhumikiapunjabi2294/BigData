Data Ingestion Pipeline using Docker Containers on Lambda using ECR, EventBridge and S3


Data Pipeline key Steps-

Build Lambda function as a Docker Container Image
●	sevir_ingestion.py - Contains code for reading data from S3 doing some transformations and generating (x_test and y_test)Shuffle and split the data into training data and test data

●	Build the Dockerfile and Tag the image
docker build -t data_ingestion .
docker tag data_ingestion:latest 711787209496.dkr.ecr.us-west-2.amazonaws.com/data_ingestion:latest

●	Create ECR repository and Push Lambda container image to ECR
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 711787209496.dkr.ecr.us-west-2.amazonaws.com
docker push 711787209496.dkr.ecr.us-west-2.amazonaws.com/data_ingestion:latest

●	Deploy Lambda container image with AWS Lambda console

●	Sync the data with sevir open registry
aws s3 sync s3://sevir/ s3://seviringestion
