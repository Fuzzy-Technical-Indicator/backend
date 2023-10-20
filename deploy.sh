#!/bin/bash

IMAGE_NAME="fuzzy-indicator-backend"

docker build -t $IMAGE_NAME .

# Check if the image build was successful
if [ $? -eq 0 ]; then
  echo "Docker image build successful."
else
  echo "Docker image build failed. Exiting."
  exit 1
fi

# Run the Docker container
docker run -d -p 8000:8000 $IMAGE_NAME

# Check if the container is running
if [ $? -eq 0 ]; then
  echo "Docker container is running. Access your backend app at http://localhost:8080"
else
  echo "Failed to start the Docker container. Exiting."
fi


