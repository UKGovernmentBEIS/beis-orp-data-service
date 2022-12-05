#! /bin/bash

account_id=$1
image_name=$2
tag="${3:-latest}"

docker buildx build --platform linux/amd64 -t pdf-to-text:$tag ~/Desktop/repos/beis-orp-data-service/lambdas/$image_name/Dockerfile
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin $account_id.dkr.ecr.eu-west-2.amazonaws.com
aws ecr create-repository --repository-name $image_name --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
docker tag  $image_name:$tag $account_id.dkr.ecr.eu-west-2.amazonaws.com/$image_name:$tag
docker push $account_id.dkr.ecr.eu-west-2.amazonaws.com/$image_name:$tag