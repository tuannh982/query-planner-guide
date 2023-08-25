#!/bin/sh

MODULE_NAME="$1"
mkdir -p "$MODULE_NAME"
mkdir -p "$MODULE_NAME/src/main/scala"
touch "$MODULE_NAME/src/main/scala/Main.scala"
echo 'name := "'"$MODULE_NAME"'"' > "$MODULE_NAME/build.sbt"
