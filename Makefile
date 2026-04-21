SHELL := /bin/bash

PYTHON ?= .venv/bin/python
PYTEST ?= $(PYTHON) -m pytest
PYTHONPATH_VALUE ?= .:src
GATE_PROFILE ?= pr
STRICT_RELEASE_READY ?= false
JSON_OUT ?=

.PHONY: help test test-targeted api smoke full-pytest ci ci-pr ci-release

help:
	@printf "%s\n" \
		"make test            # unified targeted suites + full pytest gate" \
		"make test-targeted   # unified targeted suites only" \
		"make api             # api test layer" \
		"make smoke           # smoke bundle contract + execution smoke" \
		"make full-pytest     # full repository pytest" \
		"make ci              # CI-oriented gate entry (defaults to GATE_PROFILE=pr)" \
		"make ci-pr           # PR/mainline engineering gate profile" \
		"make ci-release      # release gate profile (requires ready release evidence)"

test:
	PYTHONPATH=$(PYTHONPATH_VALUE) $(PYTHON) -m oae.cli.run_release_gates

test-targeted:
	PYTHONPATH=$(PYTHONPATH_VALUE) $(PYTHON) -m oae.cli.run_release_gates --skip-full-pytest

api:
	PYTHONPATH=$(PYTHONPATH_VALUE) $(PYTEST) tests/api -q

smoke:
	PYTHONPATH=$(PYTHONPATH_VALUE) $(PYTEST) tests/test_smoke_bundle_contract.py tests/api/test_execution_smoke.py -q

full-pytest:
	PYTHONPATH=$(PYTHONPATH_VALUE) $(PYTEST) -q

ci-pr:
	@$(MAKE) ci GATE_PROFILE=pr

ci-release:
	@$(MAKE) ci GATE_PROFILE=release

ci:
	@args=(--gate-profile "$(GATE_PROFILE)"); \
	if [[ "$(STRICT_RELEASE_READY)" =~ ^(1|true|TRUE|yes|YES)$$ ]]; then \
		args+=(--strict-release-ready); \
	fi; \
	if [[ -n "$(JSON_OUT)" ]]; then \
		args+=(--json-out "$(JSON_OUT)"); \
	else \
		args+=(--json-out "artifacts/runs/release_gate_summary_$(GATE_PROFILE).json"); \
	fi; \
	PYTHONPATH=$(PYTHONPATH_VALUE) $(PYTHON) -m oae.cli.run_release_gates "$${args[@]}"
