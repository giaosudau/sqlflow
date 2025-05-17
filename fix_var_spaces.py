#!/usr/bin/env python3
import re

# Example of variable syntax with spaces
incorrect = '$ { target_region | global } == "us-east"'


# Function to fix variable references by removing spaces
def fix_variable_references(condition):
    # Replace ${ var_name } with ${var_name}
    fixed = re.sub(r"\$\s*{\s*([^}]+?)\s*}", r"${\1}", condition)
    return fixed


# Test the function
fixed = fix_variable_references(incorrect)
print(f"Original: {incorrect}")
print(f"Fixed: {fixed}")

# Test with the evaluator pattern (what the evaluator would match)
pattern = r"\$\{([^}]+)\}"
var_matches_before = re.findall(pattern, incorrect)
var_matches_after = re.findall(pattern, fixed)

print(f"\nVariable matches before fix: {var_matches_before}")
print(f"Variable matches after fix: {var_matches_after}")
