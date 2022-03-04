#!/bin/sh
set -e

dotnet test --no-build -v n --logger "console;verbosity=normal" < /dev/null
