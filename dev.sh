#!/bin/sh

set -e

watchexec -r -e .go -- go run . "${@}"
