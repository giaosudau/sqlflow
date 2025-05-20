# Two-Stage Release Process Diagram

## Overview

The SQLFlow release process follows a two-stage approach, with TestPyPI serving as a validation environment before promoting to PyPI.

## Diagram

```
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│                │     │                │     │                │
│   Local Dev    │────▶│    TestPyPI    │────▶│      PyPI      │
│                │     │                │     │                │
└────────────────┘     └────────────────┘     └────────────────┘
       │                      │                      │
       │                      │                      │
       ▼                      ▼                      ▼
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│                │     │                │     │                │
│  ./tag_release │     │  Test Package  │     │ ./release_to   │
│     .sh        │     │  Installation  │     │    _pypi.sh    │
│                │     │                │     │                │
└────────────────┘     └────────────────┘     └────────────────┘
       │                      │                      │
       │                      │                      │
       ▼                      ▼                      ▼
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│                │     │                │     │                │
│ Tag Created:   │     │ Quality Check  │     │ Tag Created:   │
│     v0.1.5     │     │   Smoke Tests  │     │  v0.1.5-prod   │
│                │     │                │     │                │
└────────────────┘     └────────────────┘     └────────────────┘
       │                      │                      │
       │                      │                      │
       ▼                      ▼                      ▼
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│                │     │                │     │                │
│GitHub Action   │     │   Verification │     │GitHub Action   │
│Triggered       │     │   Complete     │     │Triggered       │
│                │     │                │     │                │
└────────────────┘     └────────────────┘     └────────────────┘
```

This diagram illustrates:

1. Local development and version bump trigger a TestPyPI release
2. Testing and validation occur on the TestPyPI package
3. After passing verification, the package is promoted to PyPI
4. A GitHub release documents the production release

This approach minimizes the risk of releasing broken packages to the public PyPI repository.
