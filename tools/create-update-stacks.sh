#!/bin/sh

pip install virtualenv -q
virtualenv venv
source venv/bin/activate
pip install -r tools/requirements.txt -q


stack_suffix="master"
if [ $# -eq 0 ]
	then
    	echo "No stack suffix argument provided. Using master."
   	else
   		stack_suffix="$1"
fi

python tools/create-update-stack.py "datalake-sagemaker" "templates/datalake-sagemaker.template" "ci/datalake-sagemaker.json" $stack_suffix

deactivate