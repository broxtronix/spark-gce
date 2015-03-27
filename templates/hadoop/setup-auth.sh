#!/bin/bash

PROJECT_ID=`gcloud config list --format json | python -c 'import sys, json; print json.load(sys.stdin)["core"]["project"]'`
echo $PROJECT_ID

sed -i "s/{{google-project-id}}/$PROJECT_ID/g" $HOME/ephemeral-hdfs/conf/core-site.xml

