#!/bin/sh
set -e

cd ../NBXplorer.Tests
docker-compose -v
docker-compose down --v
docker-compose build
docker-compose run tests
