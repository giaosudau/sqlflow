import pandas as pd

from sqlflow.udfs.decorators import python_table_udf


# Test with explicit schema
@python_table_udf(
    output_schema={"price": "DOUBLE", "quantity": "INTEGER", "total": "DOUBLE"}
)
def calculate_total(df):
    result = df.copy()
    result["total"] = result["price"] * result["quantity"]
    return result


# Test with schema inference
@python_table_udf(infer=True)
def infer_schema(df):
    result = df.copy()
    result["category"] = "test"
    result["is_expensive"] = result["price"] > 25
    return result


# Create test data
df = pd.DataFrame({"price": [10.0, 20.0, 30.0], "quantity": [2, 3, 4]})

# Test explicit schema
result1 = calculate_total(df)
print("Explicit schema test:")
print(result1)
print("Output schema:", calculate_total._output_schema)

# Test schema inference
result2 = infer_schema(df)
print("\nSchema inference test:")
print(result2)
print("Inferred schema:", infer_schema._output_schema)

# Test schema validation failure
try:

    @python_table_udf(output_schema={"price": "DOUBLE", "missing": "INTEGER"})
    def invalid_schema(df):
        return df  # Missing the 'missing' column

    invalid_schema(df)
except ValueError as e:
    print("\nExpected error with invalid schema:")
    print(str(e))
