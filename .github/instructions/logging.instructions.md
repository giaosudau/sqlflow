---
applyTo: "**"
---
# Logging Standards

- Use the built-in `logging` module for all logging.
- Do not use `print()` for logging purposes.
- Use appropriate logging levels: `debug`, `info`, `warning`, `error`, `critical`.
- Include contextual information in log messages where helpful.
- Configure loggers at the entry point of the application, not in libraries or modules.
- Avoid logging sensitive information.