# SQLFlow Documentation Style Guide

This style guide provides guidelines for writing clear, consistent, and effective documentation for SQLFlow.

## Command Line Examples

### Pipeline Commands

When documenting pipeline commands, always use just the pipeline name without path or file extension:

```bash
# Correct
sqlflow pipeline run example

# Incorrect - Do not use full path or file extension
sqlflow pipeline run pipelines/example.sf
```

This applies to all pipeline commands:
- `sqlflow pipeline run`
- `sqlflow pipeline compile`
- `sqlflow pipeline validate`
- `sqlflow pipeline list`

### Variables

For command variables, use the JSON format with single quotes:

```bash
# Correct
sqlflow pipeline run example --vars '{"date": "2023-10-25", "region": "us-east"}'

# Incorrect
sqlflow pipeline run example --vars date=2023-10-25
```

### Profiles

When specifying profiles, use the `--profile` flag:

```bash
# Correct
sqlflow pipeline run example --profile production

# Incorrect
sqlflow pipeline run example -p production
```

## Code Blocks

Always include the language identifier for code blocks:

````markdown
```sql
SELECT * FROM users;
```

```bash
sqlflow pipeline run example
```

```python
def my_function():
    return "Hello World"
```
````

## CLI Command Structure

Use the following format when describing CLI commands:

```
sqlflow <command-group> <command> [arguments] [options]
```

For example:
```
sqlflow pipeline run example --profile production --vars '{"date": "2023-10-25"}'
```

## Additional Guidelines

- Use backticks around command names, file names, and technical terms
- Use bold for UI elements and important concepts
- Use italics for emphasis
- Use sentence case for headings
- Include examples for all commands
- Always test commands before documenting them

By following these guidelines, we can ensure our documentation is consistent, clear, and accurate.
