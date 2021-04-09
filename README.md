<!--
title: 'AWS Serverless REST API with DynamoDB store and presigned URLs example in Python 3.8.'
description: 'This example demonstrates how to setup a RESTful Web Service. DynamoDB is used to store the data.'
layout: Doc
framework: v1
platform: AWS
language: Python
authorLink: 'https://github.com/bedge'
authorName: 'Bruce Edge'
authorAvatar: 'https://avatars1.githubusercontent.com/u/499317?v=4&s=140'
-->
# Serverless REST API
This demonstrates how to setup a [RESTful Web Service](https://en.wikipedia.org/wiki/Representational_state_transfer#Applied_to_web_services) 

### API GW Integration model
All methods use `lambda-proxy` integration as that reduces the API GW interference in the payload.
### Logging
The log_cfg.py is an alternate way to setup the python logging to be more friendly wth AWS lambda.
The lambda default logging config is to not print any source file or line number which makes it harder to correleate with the source.

Adding the import:
```python
    from log_cfg import logger
```
at the start of every event handler ensures that the format of the log messages are consistent, customizable and all in one place. 

Default format uses:
```python
'%(asctime)-15s %(process)d-%(thread)d %(name)s [%(filename)s:%(lineno)d] :%(levelname)8s: %(message)s'
```

## Setup

```bash
npm install -g serverless

git clone https://github.com/benlau6/mtr-hk-api.git
cd ./mtr-hk-api

npm install
```

## Deploy

In order to deploy the endpoint simply run

```bash
sls deploy
```

### Upload a file to the URL

In order to deploy the endpoint simply run

```bash
sls deploy -f $function
```

## Docker

In order to run it locally, run the script below:

```bash
docker-compose up
```

Then go to 
1. 127.0.0.1/dev to see if the endpoint is accessible
2. 127.0.0.1/dev/docs to see api documentation
3. 127.0.0.1/dev/items to query dynamoDB
4. 127.0.0.1/dev/graphql to explore graphql