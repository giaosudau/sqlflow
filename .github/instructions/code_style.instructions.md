---
applyTo: '*.py'
---
# SQLFlow Code Style Guidelines

## Development Environment
- Use a `.venv` virtual environment with fish shell.
- Never skip pre-commit hooks; they enforce our standards.

## Naming Conventions (PEP 8)
- Use `snake_case` for functions, variables, and module names.
- Use `PascalCase` for classes, exceptions, and type aliases.
- Prefix internal helpers with a single underscore: `_private_method`.
- Use `SCREAMING_SNAKE_CASE` for module-level constants.
- Avoid single-letter names (except for loop indices) and ambiguous abbreviations.

