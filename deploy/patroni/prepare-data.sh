#!/usr/bin/env bash
set -euo pipefail

chown postgres:postgres "$1"
chmod 700 "$1"
