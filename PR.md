# Fix SQL formatting issues in query generation

## What
This PR fixes two critical bugs in the SQL query formatting:
1. Incorrect spacing around dot operators in table.column references
2. Incorrect spacing between function names and parentheses in SQL function calls

Both issues would have caused SQL syntax errors during execution.

## Why
In compiled SQL queries, there were two critical formatting problems:
1. Table and column references had spaces around the dot:
   - Incorrect: `SELECT t . id, c . name FROM table`
   - Correct: `SELECT t.id, c.name FROM table`

2. Function calls had spaces between the function name and opening parenthesis:
   - Incorrect: `COUNT ( DISTINCT user_id)`
   - Correct: `COUNT(DISTINCT user_id)`

These spacing issues would cause SQL syntax errors when executing the queries against most database engines.

## How
### Dot Operator Fix:
1. Added a DOT token type to the lexer to properly recognize dots in SQL
2. Implemented special handling for DOT tokens in SQL query formatting

### Function Call Fix:
1. Enhanced the SQL formatter to detect function calls and properly format them
2. Added regular expression rules to remove spaces:
   - Between function names and opening parentheses
   - Between parentheses and their content

3. Created a robust `_format_sql_query` method that properly joins tokens for both fixes
4. Added comprehensive tests to verify both fixes

## Testing
- Added unit tests specifically for dot operator formatting
- Added unit tests for function call formatting
- Verified all existing parser tests still pass
- Manually tested SQL parsing with various SQL constructs
- Confirmed the fixes resolve the issues in compiled JSONs

## Changes
- Modified `sqlflow/parser/lexer.py` to add DOT token type
- Modified `sqlflow/parser/parser.py` to handle DOT tokens and function calls in SQL generation
- Added `tests/unit/parser/test_lexer_dot_operator.py` with specific tests for both issues
- Added documentation comments explaining the importance of proper SQL formatting
- Updated CHANGELOG.md to document the fixes 