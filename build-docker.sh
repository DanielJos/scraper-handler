#!/bin/bash

docker build -t cnst-jobhandler .
# docker tag cnst-jobhandler:latest "${ECR_URL}"/cnst-jobhandler:latest
# docker push "${ECR_URL}"/cnst-jobhandler:latest

# docker run -it cnst-jobhandler 