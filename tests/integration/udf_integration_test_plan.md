# UDF Integration Test Plan

This document outlines the integration tests for Python UDFs in SQLFlow, focusing on real-world scenarios and edge cases.

## MVP Test Focus

For the MVP release, we will focus on ensuring that basic UDF functionality works reliably in real-world scenarios:

1. ✅ Basic scalar UDF functionality 
2. ✅ Basic table UDF functionality
3. ✅ Complex data type handling
4. ✅ Edge case handling (NULL, empty datasets, large values)
5. ⬜ Error handling and recovery

## Test Categories

### 1. Data Type Tests

- ✅ **Test scalar UDFs with various data types**
  - ✅ Test with integers, floats, strings, booleans
  - ⬜ Test with date/time values
  - ✅ Test with NULL values
  - ✅ Test with special characters in strings

- ✅ **Test table UDFs with complex data structures**
  - ✅ Test with DataFrames containing mixed data types
  - ✅ Test with NULL values in various columns
  - ✅ Test with empty DataFrames
  - ⬜ Test with large DataFrames (performance test)

### 2. Parameter Handling Tests

- ✅ **Test scalar UDFs with different parameter combinations**
  - ✅ Test with single parameter
  - ✅ Test with multiple parameters
  - ✅ Test with optional parameters
  - ✅ Test with default parameter values

- ✅ **Test table UDFs with different parameter configurations**
  - ✅ Test with DataFrame-only parameter
  - ✅ Test with DataFrame + additional parameters
  - ✅ Test with keyword arguments

### 3. Real-world Scenario Tests

- ✅ **E-commerce data analysis**
  - ✅ Test customer segmentation with table UDFs
  - ✅ Test pricing calculations with scalar UDFs
  - ✅ Test order analysis with combined UDFs

- ⬜ **Data cleaning and transformation**
  - ⬜ Test text normalization UDFs
  - ⬜ Test data validation UDFs
  - ⬜ Test data reshaping with table UDFs

### 4. Error Handling Tests

- ⬜ **Test error conditions in UDFs**
  - ⬜ Test UDFs that raise exceptions
  - ⬜ Test error reporting clarity
  - ⬜ Test error handling in complex pipelines

### 5. Performance Tests

- ⬜ **Test UDF performance with various data sizes**
  - ⬜ Test with small datasets
  - ⬜ Test with medium datasets (hundreds of rows)
  - ⬜ Test with large datasets (thousands of rows)

## Implementation Plan

We will implement these tests in the following order:

1. ✅ Data type tests (basic tests first)
2. ✅ Parameter handling tests 
3. ✅ Real-world scenario tests (e-commerce focus)
4. ⬜ Error handling tests
5. ⬜ Performance tests (if time permits)

Each test will be added incrementally, with a focus on testing real use cases that users will encounter.

## Completed Tests

1. ✅ **test_udf_data_types.py** - Tests scalar and table UDFs with various data types and NULL handling
   - Tests integer handling with scalar UDFs
   - Tests string handling with scalar UDFs
   - Tests NULL handling with table UDFs 

2. ✅ **test_udf_parameters.py** - Tests UDFs with different parameter configurations
   - Tests scalar UDFs with single, multiple, and optional parameters
   - Tests table UDFs with different parameter setups

3. ✅ **test_udf_ecommerce.py** - Tests UDFs in real-world e-commerce scenarios
   - Tests customer segmentation with scalar UDFs
   - Tests pricing/discount calculations with scalar UDFs
   - Tests customer analytics with table UDFs
   - Tests product performance analysis with table UDFs
   - Tests a full e-commerce analysis pipeline with multiple UDFs 