#!/bin/bash

sudo rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/* && \
sudo apt-get clean && \
sudo apt-get update && \
sudo apt-get install poppler-utils tesseract-ocr -y
