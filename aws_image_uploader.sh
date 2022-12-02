#! /bin/bash
echo "Account ID:"
read account_id
echo "Image Name:"
read image_name

aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin $account_id.dkr.ecr.eu-west-2.amazonaws.com
aws ecr create-repository --repository-name $image_name --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
docker tag  $image_name:latest $account_id.dkr.ecr.eu-west-2.amazonaws.com/$image_name:latest
docker push $account_id.dkr.ecr.eu-west-2.amazonaws.com/$image_name:latest