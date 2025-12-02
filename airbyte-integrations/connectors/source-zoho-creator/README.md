# source-zoho-creator

Airbyte source connector to extract data from Zoho Creator applications using the Zoho Creator Data API.

## Overview

This repository contains an Airbyte source connector that pulls records from Zoho Creator applications and exposes them to Airbyte destinations. It is implemented in Python and uses the Airbyte CDK.

## Project layout

- Python package at the repository root.
- Connector implementation and tests follow Airbyte connector conventions.

## Requirements

- Python >= 3.10 and < 3.14 (as specified in `pyproject.toml`).
- Poetry for dependency and environment management.
- Docker (optional) to build connector images for use with Airbyte.

## Install (Development)

1. Install Poetry (if not already installed). See https://python-poetry.org/docs/

2. Create a virtual environment and install dependencies:

```bash
poetry install
```

3. Activate a shell in the project's virtual environment (optional but useful):

```bash
poetry shell
```

## Configuration

This connector requires credentials to access the Zoho Creator API. Depending on how you run the connector (locally via tests, or inside Airbyte), provide credentials via environment variables, a `.env` file, or via the Airbyte UI.

Typical configuration values (example names — adapt to your environment):

- ZOHO_CLIENT_ID
- ZOHO_CLIENT_SECRET
- ZOHO_REFRESH_TOKEN
- ZOHO_BASE_URL (if using a custom or region-specific endpoint)
- APPLICATION_ID or other identifiers the connector expects

Note: The exact key names and authentication flow are implemented in the connector code. Use the Airbyte UI connector configuration or the `secrets`/`env` pattern when running locally.

## Usage

Run unit tests:

```bash
poetry run pytest -q
```

Run a single module or script (example):

```bash
poetry run python -m source_zoho_creator.entrypoint --config <path-to-config.json>
```

(Replace the module path or entrypoint name above with the actual connector entrypoint in this repo if different.)

Build a Docker image for use with Airbyte (optional):

```bash
# Example (adapt tag and Dockerfile location if present)
docker build -t airbyte/source-zoho-creator:dev .
```

## Testing

- Unit tests: `poetry run pytest`
- Add tests in the repository under a `tests/` directory following the project conventions.

## Development notes

- The project uses the Airbyte CDK. See Airbyte docs for connector development guidance.
- Follow the repository's `pyproject.toml` for dependency and Python version constraints.

## Contributing

Contributions are welcome. Please open issues or pull requests. Keep changes small and include tests for new behavior.

## License

This project is licensed under the MIT License (see `pyproject.toml`).

---

If you'd like, I can refine the README with more specific examples (exact config keys, example config JSON, or how to run the connector inside the Airbyte dev environment). Tell me which details you'd like added.