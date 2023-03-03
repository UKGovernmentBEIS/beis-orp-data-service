#! /bin/bash
# Run script from the root directory of the repo
# Usage:
# sh utils/aws_image_uploader.sh <account_id> <image_name> <image_tag>


account_id=$1
image_name=$2
image_name_underscored=$(echo $image_name | tr - _)
tag="${3:-latest}"


docker buildx build --platform linux/amd64 -t $image_name:$tag  -f ./lambdas/$image_name_underscored/Dockerfile ./lambdas/$image_name_underscored/
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin $account_id.dkr.ecr.eu-west-2.amazonaws.com
aws ecr create-repository --repository-name $image_name --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
docker tag $image_name:$tag $account_id.dkr.ecr.eu-west-2.amazonaws.com/$image_name:$tag
docker push $account_id.dkr.ecr.eu-west-2.amazonaws.com/$image_name:$tag
docker rmi $(docker images -f "dangling=true" -q)
