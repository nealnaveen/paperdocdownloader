#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 508582898882.dkr.ecr.us-east-1.amazonaws.com
# Check if there are any Docker images present
if [ "$(docker images -aq)" ]; then
    echo "Removing all Docker images..."
    docker rmi -f $(docker images -aq)
else
    echo "No Docker images to remove."
fi
# Define variables
IMAGE_NAME="ptab"
REPOSITORY_URI="508582898882.dkr.ecr.us-east-1.amazonaws.com/paperdocrepo"
TAG="latest"
REPO_TAG="latest"
# Build the Docker image
echo "Building the Docker image..."
docker build -t $IMAGE_NAME .

# Tag the image
echo "Tagging the image..."
docker tag $IMAGE_NAME:$TAG $REPOSITORY_URI:$REPO_TAG



# Tag the Docker image again (in case it was removed during the build process)
echo "Tagging the Docker image again..."
docker tag $IMAGE_NAME:$TAG $REPOSITORY_URI:$TAG

# Push the Docker image to ECR
echo "Pushing the Docker image to ECR..."
docker push $REPOSITORY_URI:$TAG

echo "Script completed successfully."
