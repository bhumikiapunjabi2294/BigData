Serverless FastAPI with AWS Lambda and API Gateway
Create FastAPI SEVIR App
File Tree is as follows:



FastAPI Authentication 
Implemented With OAuth2, used JSON web tokens (JWT) for the token in the OAuth flow

In SWAGGER  testing endpoint by clicking on the GET / Root route and then clicking execute.

Deploy Swagger to Cloud

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
