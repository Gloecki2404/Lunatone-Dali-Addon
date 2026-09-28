#!/usr/bin/with-contenv bashio
# shellcheck shell=bash
bashio::log.info "Starting Lunatone DALI-2 IoT Manager..."
export WWW_DIR=/www
export DATA_DIR=/data
exec python3 -u /app/main.py
