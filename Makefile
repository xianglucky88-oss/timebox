PY=python3
SRC=src
export PYTHONPATH := $(SRC)$(if $(PYTHONPATH),:$(PYTHONPATH),)

run:
	$(PY) -m timebox.cli

backfill:
	$(PY) -m timebox.cli backfill --start 2026-11-01 --end 2026-11-04 --unit day

status:
	$(PY) -m timebox.cli status --start 2026-11-01 --end 2026-11-04 --unit day

test:
	$(PY) -m pytest -q

clean:
	rm -rf .pytest_cache **/__pycache__ *.db
