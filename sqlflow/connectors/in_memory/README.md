# In-Memory Connector

The In-Memory connector is a special-purpose connector designed primarily for **testing and development**. It allows you to treat a global, in-memory Python dictionary as a data source or destination, which is extremely useful for writing fast, isolated unit and integration tests without any external database dependencies.

## Overview

This connector is not intended for production use. Its main purpose is to provide a simple and fast way to:
- Simulate a database table by storing a pandas DataFrame in memory.
- Read data from this in-memory "table" as a source.
- Write data to this in-memory "table" as a destination.

## Documentation

- **[In-Memory Source](./SOURCE.md)**: Documentation for reading from the in-memory store.
- **[In-Memory Destination](./DESTINATION.md)**: Documentation for writing to the in-memory store. 