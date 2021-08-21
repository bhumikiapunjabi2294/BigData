**SEVIR Dataset- Data as a Service**

**Implemented Serverless FastAPI with AWS Lambda and API Gateway to make it availabe on Cloud**
The Serverless FastAPI will be ran on an AWS Lambda by using Mangum and AWS API Gateway will handle routing all requests to the Lambda.

API Gateway uses wildcard for an endpoint and redirect to the FastAPI Lambda and internally route the request accordingly. This way the only cost incurred is whenever there is a request made to the API.

Mangum allows to wrap the API with a handler that we will package and deploy as a Lambda function in AWS. Then using AWS API Gateway we will route all incoming requests to invoke the lambda and handle the routing internally within our application.

**Requirements**
Python
Virtualenv
AWS Account

Create FastAPI App including 2 Routes for Nowcast FastApi and Synrad FastApi
File Tree is as follows:

**Implemented FastAPI Authentication With OAuth2, used JSON web tokens (JWT) for the token in the OAuth flow**
Testing endpoint by clicking on the GET and POST route and then click to execute, to infer the data using-
SWAGGER
![image](https://user-images.githubusercontent.com/59785209/130325381-ed8466a2-3243-45d5-b62b-95610a874d6b.png)

POSTMAN
![image](https://user-images.githubusercontent.com/59785209/130325455-16c5a4d1-ac51-4b3a-82bc-9564b4871965.png)

**Deploy Swagger to Cloud**

Created a FastAPI application and converted it to an AWS Lambda. Setup API Gateway to route all requests to Lambda proxy and deployed Lambda Proxy API
Finally deploy the API in order for the endpoint to be public.


Test the deployed url using both:
Swagger
Postman

Streamlit Integration,Inference of Data generated in local system


Deploying Streamlit app to EC2 instance
EC2 is an elastic cloud compute that acts as a virtual server that operate in the cloud. Let us see the steps of deploying our streamlit app.
 
STEPS
1.Create An EC2 Instance on AWS
2.Configure Our Custom TCP port to 8501/02
3.SSH into Our Instance/AMI
4.Setup our python environment and run
5.Keeping the App Continuously Running
6.Configure a custom domain name
