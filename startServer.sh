#!/usr/bin/env sh
set -e

export NGAMS_CONF=/NGAS/cfg/ngamsServer.conf
ngamsServer -cfg ${NGAMS_CONF} -v 4 -autoonline -force
