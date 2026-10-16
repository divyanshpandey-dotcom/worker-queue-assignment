.PHONY: seed worker

# The grader's two targets. Both read PROVIDER_URL and DATABASE_URL from the
# environment and default to the 127.0.0.1 services in README.md.

seed:
	uv run python -m dispatcher.seed

# Foreground, no fork, no setsid: the grader kills the whole process group.
worker:
	uv run python -m dispatcher.worker
