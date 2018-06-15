#!/bin/sh

lambdaFunction=$1
lambda=$2
bucketName="datalake-sagemaker-lambda-code"

source="src/$lambda"
filename="$lambda.zip"
package_folder="packages"
destination="$package_folder/$filename"

cd $source

s3Key="$lambda/$filename"

npm install
zip -r $lambda .

cd "../.."

mkdir -p $package_folder
mv "$source/$filename" $package_folder
aws s3 cp $destination "s3://$bucketName/$s3Key"
aws lambda update-function-code --function-name $lambdaFunction --s3-bucket $bucketName --s3-key $s3Key