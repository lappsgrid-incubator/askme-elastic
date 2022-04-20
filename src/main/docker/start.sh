#!/usr/bin/env bash

if [[ -n "$1" ]] ; then
    IP=$1
fi

java -Xmx4G -jar service.jar
