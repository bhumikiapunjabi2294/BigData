**SEVIR Dataset- Data as a Service- Swagger and Streamlit Integration**

1- **Implemented Serverless FastAPI with AWS Lambda and API Gateway to make it availabe on Cloud**

Created a FastAPI application and converted it to an AWS Lambda. Setup API Gateway to route all requests to Lambda proxy and deployed Lambda Proxy API
Finally deployed the API in order for the endpoint to be public.

The Serverless FastAPI will be ran on an AWS Lambda by using Mangum and AWS API Gateway will handle routing all requests to the Lambda.

API Gateway uses wildcard for an endpoint and redirect to the FastAPI Lambda and internally route the request accordingly. This way the only cost incurred is whenever there is a request made to the API.

Mangum allows to wrap the API with a handler that we will package and deploy as a Lambda function in AWS. Then using AWS API Gateway we will route all incoming requests to invoke the lambda and handle the routing internally within our application.

**Requirements**
Python
Virtualenv
AWS Account

Create FastAPI App including 2 Routes for Nowcast FastApi and Synrad FastApi
File Tree is as follows:

![image](https://user-images.githubusercontent.com/59785209/130325792-dad1c806-30c9-4362-9b91-52859736fd01.png)

**Implemented FastAPI Authentication With OAuth2, used JSON web tokens (JWT) for the token in the OAuth flow**
Testing endpoint by clicking on the GET and POST route and then click to execute, to infer the data using-
SWAGGER
POSTMAN

2- **Deploy Swagger to Cloud**

Upload Zip File to S3

Create AWS Lambda

Upload the zip file from S3 to the created function.

Update Handler to app.main.handler

Test FastAPI Lambda by changing test event-
![image](https://user-images.githubusercontent.com/59785209/130325784-314f85a7-66f9-420b-baf4-d01d59b66a9a.png)

Create API Gateway, with resources and required methods.

Deploy Lambda Proxy API, for the endpoint to be public

Finally We Created a FastAPI application and converted it to an AWS Lambda. Then we setup API Gateway to route all requests to our Lambda proxy
![image](https://user-images.githubusercontent.com/59785209/130325867-291d13d7-6c49-45dc-a4d3-af759d3aa1d2.png)

**Test the deployed url on:**
**Swagger**

![image](https://user-images.githubusercontent.com/59785209/130325381-ed8466a2-3243-45d5-b62b-95610a874d6b.png)

**Postman**

![image](https://user-images.githubusercontent.com/59785209/130325455-16c5a4d1-ac51-4b3a-82bc-9564b4871965.png)

**This way we can get Nowcast and Synrad Output Data at provided Index**


3- **Streamlit Integration**


Created python file to do the Inference of Nowcast and Synrad Data locally.
![image](https://user-images.githubusercontent.com/59785209/130325946-c627a757-804a-4a6f-a632-f6207673fd34.png)


4- **Deploying Streamlit app to EC2 instance**
EC2 is an elastic cloud compute that acts as a virtual server that operate in the cloud. Let us see the steps of deploying our streamlit app.
 
**STEPS**

Create An EC2 Instance on AWS

Configure Our Custom TCP port to 8501/02

SSH into Our Instance/AMI

Setup our python environment and run

Keeping the App Continuously Running

Configure a custom domain name

![image](https://user-images.githubusercontent.com/59785209/130325981-978da296-d384-47ef-aa1c-f219dc52b3d7.png)

![image](https://user-images.githubusercontent.com/59785209/130325988-18b961e4-01f1-4c64-b7df-f7c8019a1a71.png)

Lastly install **tmux** (sudo yum install tmux) on the running ec2-user and create new window using cmd

tmux a -s mywindow

This way the app is hosted even after the terminal is closed, so this works like AWS
