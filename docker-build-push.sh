#!/bin/bash

IMAGE_NAME="calendar-rag";
ARTIFACT_REPO="us-east1-docker.pkg.dev/themothershp/themothershp";

docker image build . -t ${IMAGE_NAME};

docker image tag ${IMAGE_NAME}:latest ${ARTIFACT_REPO}/${IMAGE_NAME}:latest;

docker image push ${ARTIFACT_REPO}/${IMAGE_NAME}:latest;

gcloud artifacts docker images list ${ARTIFACT_REPO}/${IMAGE_NAME} --include-tags
