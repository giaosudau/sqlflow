---
applyTo: '*.py'
---
# Debugging and Error Handling Standards

## Error Handling

- Use specific exception types; avoid generic `except:` blocks.
- Make error messages informative and actionable.
- Include context in exception messages.
- Create custom exceptions for domain-specific errors.

## Debugging

- Use `logger.debug()` for trace-level logging.
- For temporary debugging, use `import pdb; pdb.set_trace()` (local only).
- Remove all debugging breakpoints before submitting PRs.
- Use pretty printing (`pprint.pformat`) for complex data structures.

## Root Cause Analysis

- Apply the "5 Whys" technique for recurring issues.
- Document findings in the issue tracker with code links.
- For complex failures, create a debugging report with:
    - Issue Description
    - Observed Behavior
    - Root Cause
    - Solution

## Troubleshooting Tools

- Use logging to trace execution flow.
- Log SQL statements in debug mode for database operations.
- Implement validation checks before critical operations.
- Consider debug-only diagnostic endpoints for development.

## Performance Debugging

- Isolate bottlenecks with timing instrumentation (`time.time()`).
- Track step durations with decorators.
- Use profiling tools (e.g., `cProfile`) for deeper analysis.

## Documentation of Known Issues

- Maintain a `TROUBLESHOOTING.md` for common issues.
- Include steps to reproduce, symptoms, and solutions.
- Add debug logs for known edge cases.
- Add comments for non-obvious workarounds.