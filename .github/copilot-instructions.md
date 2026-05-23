# Copilot Instructions

## Language Policy

- Use English for all source code, comments, identifiers, tests, documentation, and commit messages.
- Keep naming and technical communication in English across the entire repository.

## Exception

- Grafana may be configured with German as the default language where supported by provisioning or environment configuration.
- Do not apply German as a default language outside Grafana.

## Security and Secrets

- Never hard-code secrets (passwords, tokens, keys, connection secrets) in tracked files.
- Store secrets only in `.env` (local, untracked).
- Keep `.env.example` as the committed template with placeholder values.
- Any new service requiring credentials must consume them from environment variables.
