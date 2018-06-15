#!/bin/sh

lambda=$1
bucketName="s3://datalake-sagemaker-lambda-code"

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
aws s3 cp $destination "$bucketName/$s3Key"