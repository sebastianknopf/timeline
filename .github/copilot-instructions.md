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

## Processor Code Standards

- Use type hints everywhere in processor code, including function signatures, class attributes, return values, and public constants.
- Treat missing type hints as a correctness issue and add or refine typing during changes.
- Implement processor features using object-oriented design.
- Prefer explicit classes and interfaces for pipelines, scheduler components, clients, and loaders over procedural-only implementations.
- Prefer `asyncio`-based implementations wherever possible, especially for scheduler orchestration, HTTP I/O, database I/O, and other I/O-bound workflows.
- Avoid blocking calls in async code paths. If a blocking library is unavoidable, isolate it behind clear boundaries (for example executor-based wrappers) so the event loop remains responsive.
- Keep async boundaries explicit in interfaces (`async def`, awaited calls, and cancellation-aware task handling) to support safe concurrent pipeline execution.
- Enforce strict separation of concerns. Re-usable logic must be encapsulated in dedicated modules so it can be re-used across components.
- Create unit tests for every module.
- Use only Python's standard `unittest` package for test implementation.
- For local testing, run tests only with the local project virtual environment.
