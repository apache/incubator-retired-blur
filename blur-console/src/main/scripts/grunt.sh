#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"

# Check for grunt-cli installed: log `npm install -g grunt-cli`
command -v grunt >/dev/null 2>&1 || { echo >&2 "[ERROR] grunt not found! install: npm install -g grunt-cli"; exit 1; }

# run `npm install` in src/main/webapp
cd $DIR/../webapp >/dev/null
npm install --quiet

grunt $1

cd - >/dev/null

