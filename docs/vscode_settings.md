# VSCode Settings for SQLFlow Development

To ensure consistent code formatting and linting in VSCode, add the following settings to your `.vscode/settings.json` file:

```json
{
  "editor.formatOnSave": true,
  "python.formatting.provider": "black",
  "python.linting.enabled": true,
  "python.linting.flake8Enabled": true,
  "python.linting.flake8Args": [
    "--config=setup.cfg",
    "--ignore=E501,E221,E222,E223,E224,E303,E301,E302,E402,F401,F841,E741,W291,W293,D100,D101,W503,W504,E203",
    "--max-complexity=10"
  ],
  "editor.codeActionsOnSave": {
    "source.organizeImports": true,
    "source.fixAll": true
  },
  "[python]": {
    "editor.defaultFormatter": "ms-python.black-formatter",
    "editor.formatOnSave": true
  },
  "isort.args": ["--profile", "black"],
  "autoflake.args": [
    "--in-place",
    "--remove-all-unused-imports",
    "--remove-unused-variables",
    "--expand-star-imports",
    "--ignore-init-module-imports"
  ]
}
```

## Required Extensions

Install the following VSCode extensions for the best development experience:

- Black Formatter (`ms-python.black-formatter`)
- Flake8 (`ms-python.flake8`)
- isort (`ms-python.isort`)
- autoflake (available through Python extension)

## Save Actions

These settings will automatically run the following commands on save:

- `black .` - Format Python code
- `isort` - Sort imports
- `flake8 --config=setup.cfg .` - Lint Python code
- `autoflake` - Remove unused imports and variables

This ensures consistent code style across the project and helps catch issues early.
