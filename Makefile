# nanocloud Makefile — a fluxor-only project. There is NO host crate: the
# apiserver (api_ingress), every controller/reconciler, and the CLI are PIC
# fmods in `modules/app/*`, composed into fluxor graphs in `packaging/`. Each
# lifecycle target delegates to its `fluxor` CLI verb, which reads this
# project's shape (fluxor.toml) — a make body that re-implements the verb is
# drift, not convenience (cli.md §1).

.PHONY: help build test lint ci publish clean
SHELL       := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := build

help:
	@fluxor help --make

build:
	fluxor build

test:
	fluxor test

lint:
	fluxor lint

ci:
	fluxor ci

publish:
	fluxor publish

clean:
	fluxor clean
