# Copilot Instructions

## Documentation Behavior

- Treat the documentation as the primary definition of the project structure and architecture.
- When the task is about project shape, service layout, ports, or deployment topology, update the architecture docs first instead of generating code.
- Keep the high-level repository architecture in `docs/ARCHITECTURE.md` and service-specific architecture details in dedicated follow-up documents such as `docs/PROCESSOR.md`.
- Prefer cross-links between documentation files so the repository overview stays navigable.

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
