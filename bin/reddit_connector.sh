#!/usr/bin/env bash

docker stop redditconnector

docker container rm redditconnector

docker build ../github/ -t redditconnector --name redditconnector

docker run --env-file ../github/reddit.env --network tap -it redditconnector
