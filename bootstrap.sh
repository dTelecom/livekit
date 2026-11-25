#!/bin/sh
set -eu
cd "$(dirname "$0")"
go mod download
go install -mod=readonly github.com/magefile/mage
