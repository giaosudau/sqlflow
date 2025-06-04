# Debug Scripts Directory

This directory contains development and debugging scripts used during SQLFlow Phase 2 development.

## Scripts

- `test_key_mode.py` - S3 connector key-based access testing
- `test_parse.py` - Pipeline parsing validation  
- `test_s3_connection.py` - S3 connectivity debugging
- `test_s3_direct.py` - Direct S3 connector testing
- `test_s3_verification.py` - S3 bucket verification utilities

## Usage

These scripts are for development purposes only and are not part of the main demo workflow.

```bash
cd debug/scripts
python3 test_key_mode.py
```

## Note

These files were moved from the root directory to improve project organization
following SQLFlow engineering principles.
