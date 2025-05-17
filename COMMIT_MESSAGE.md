fix(parser): Fix SQL formatting issues with dot operators and function calls

This commit fixes two critical bugs where SQL queries were being incorrectly 
formatted, which would cause SQL syntax errors during execution:

1. Table.column references had spaces around the dots
   - Before: `SELECT t . id, c . name FROM table`
   - After: `SELECT t.id, c.name FROM table`

2. Function calls had spaces between function names and parentheses
   - Before: `COUNT ( DISTINCT user_id )`
   - After: `COUNT(DISTINCT user_id)`

Key changes:
- Added DOT token type to lexer to recognize periods in SQL
- Enhanced SQL formatter to handle function calls correctly
- Implemented special handling for dot tokens and parentheses
- Added regex rules to remove spaces in function calls
- Created a robust _format_sql_query method for proper SQL formatting
- Added comprehensive tests for all fixes
- Updated documentation to explain SQL formatting features
- Added CHANGELOG.md to track project changes

Additional changes:
- Added README section on SQL features and best practices
- Included PR description for documenting the fixes properly

Fixes #123 (example issue number) 