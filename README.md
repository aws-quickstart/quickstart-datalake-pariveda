# quickstart-datalake-sagemaker

This GitHub repository contains a quickstart template for creating a datalake in AWS integrated with Sagemaker to derive insights using machine learning.

## Development Command Line Tools

Note: All command line tools should be run in root directory of project

- Create/Update Stack: `tools/create-update-stacks.sh [stack-suffix]`

- Upload Lambda Code to S3: `tools/upload-lambda-code.sh [lambda-name]`

- Update Lambda Code: `tools/update-lambda-code.sh [function-name] [lambda-name]`

- Send Data to Kinesis: `tools/send-kinesis-data.sh [stream-name]`