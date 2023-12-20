#!/usr/bin/env bash

mkdir -p build

rm -f ./build/*

output="{{.Dir}}-{{.OS}}-{{.Arch}}"

echo "Compiling:"
export GOFLAGS="-trimpath"

os="windows"
arch="amd64"
CGO_ENABLED=0 gox -tags "server" -ldflags="-s -w -buildid=" -os="$os" -arch="$arch" -output="$output"


mv script-boy-* ./build
cd ./build
FILES=$(find . -type f)
for file in $FILES; do
  filename=$(basename "$file")
  extension="${filename##*.}"
  if [ "$extension" != "zip" ]; then
    zip_name=$(basename "$file" ".$extension")
    if [ "$extension" == "exe" ]; then
      upx -o "script-boy.exe" "$file"
      zip -q "$zip_name.zip" "script-boy.exe"
      rm -f script-boy.exe
    else
      upx -o "script-boy" "$file"
      zip -q "$zip_name.zip" "script-boy"
      rm -f script-boy
    fi
    rm -f "$file"
  fi
done
