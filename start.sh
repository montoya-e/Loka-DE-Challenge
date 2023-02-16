#!/bin/bash

cd db-docker
sudo docker-compose up
cd ..
python main.py
