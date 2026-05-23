# Configuration Architecture

This document defines the runtime configuration model consumed by the processor.

For the overall system architecture, see [docs/ARCHITECTURE.md](ARCHITECTURE.md).

## File Format and Location

- Format: YAML only
- Host path: `config/config.yaml`
- Container path: `/app/config/config.yaml`

The repository provides a template at `config/config.yaml.example`.

## Top-Level Structure

The top-level key is `instance` and must contain at least one instance object.

Each instance defines:

- `id`: unique instance identifier
- `pipeline`: list of pipeline objects (one or more)

## Pipeline Object Structure

Each pipeline object defines:

- `id`: pipeline identifier used in logs
- `type`: pipeline type, expected values are `nominal` or `realtime`
- `cron`: cron expression used to schedule this pipeline
- `endpoint`: source endpoint URL or address
- `authentication` (optional): authentication object when required by the endpoint

Detailed behavioral semantics of `nominal` and `realtime` pipelines are documented in [docs/PROCESSOR.md](PROCESSOR.md).

Authentication supports exactly one of these shapes:

- token authentication: `token`
- basic credentials: `username` and `password`

If the endpoint does not require authentication, the `authentication` key can be omitted.

## Configuration Validation Rules

At startup, the processor must read and validate the YAML configuration before runtime execution begins.

Minimum validation requirements:

1. YAML is syntactically valid.
2. Top-level `instance` key exists and contains at least one object.
3. Each instance has non-empty `id` and at least one `pipeline`.
4. Each pipeline has non-empty `id`, `type`, `cron`, and `endpoint`.
5. Pipeline `type` is either `nominal` or `realtime`.
6. If `authentication` is present, it is either:
   - `{ token: <value> }`
   - `{ username: <value>, password: <value> }`

If validation fails, startup must fail and no pipelines are run.

## Baseline Requirement

Each instance should configure at least:

- one `nominal` pipeline
- one `realtime` pipeline

Recommended cadence:

- `nominal`: once per day (for example `0 2 * * *`)
- `realtime`: at least once per minute (for example `* * * * *`)