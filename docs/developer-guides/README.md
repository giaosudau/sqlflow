# SQLFlow Developer Documentation

## Architecture Documentation Guide

This directory contains comprehensive documentation for developers working on or extending SQLFlow. Each document has a specific role to avoid duplication and ensure maintainability.

### 📋 Document Roles

| Document | Purpose | Audience | Content |
|----------|---------|----------|---------|
| **[Technical Overview](technical-overview.md)** | Founder's vision and positioning | New contributors, stakeholders | Why SQLFlow exists, competitive landscape, human story |
| **[Architecture Overview](architecture-overview.md)** | High-level system design | Architects, senior developers | System philosophy, design principles, component overview |
| **[Architecture Deep Dive](architecture-deep-dive.md)** | Implementation specifics | Core contributors | Performance details, data flow, execution patterns |
| **[UDF System](udf-system.md)** | Python function integration | Function developers | UDF types, limitations, workarounds, future vision |
| **[Extending SQLFlow](extending-sqlflow.md)** | Practical development guide | Extension builders | How to build connectors, UDFs, integration patterns |
| **[Contributing](contributing.md)** | Development process | All contributors | Setup, testing, code review, community standards |

### 🎯 Reading Paths

**For New Contributors:**
1. [Technical Overview](technical-overview.md) - Understand the vision
2. [Architecture Overview](architecture-overview.md) - Learn system design
3. [Contributing](contributing.md) - Start developing

**For Function Developers:**
1. [UDF System](udf-system.md) - Understand UDF architecture
2. [Extending SQLFlow](extending-sqlflow.md) - Build functions and connectors
3. [Contributing](contributing.md) - Testing and submission process

**For Core Contributors:**
1. [Architecture Overview](architecture-overview.md) - System philosophy
2. [Architecture Deep Dive](architecture-deep-dive.md) - Implementation details
3. [UDF System](udf-system.md) - Component-specific architecture
4. [Contributing](contributing.md) - Development standards

### 🔄 Document Maintenance

**No Duplication Policy:**
- Each concept is documented in only one place
- Cross-references maintain connections
- Architecture Overview = philosophy and high-level design
- Architecture Deep Dive = implementation and performance
- Component docs = specific technical details

**Update Guidelines:**
- High-level concepts → Update Architecture Overview
- Implementation changes → Update Architecture Deep Dive
- UDF system changes → Update UDF System doc
- New extension patterns → Update Extending SQLFlow

### 🧭 Quick Reference

| Need to... | Read This |
|------------|-----------|
| Understand SQLFlow's purpose | [Technical Overview](technical-overview.md) |
| Learn system architecture | [Architecture Overview](architecture-overview.md) |
| Optimize performance | [Architecture Deep Dive](architecture-deep-dive.md) |
| Build custom functions | [UDF System](udf-system.md) |
| Create connectors | [Extending SQLFlow](extending-sqlflow.md) |
| Contribute code | [Contributing](contributing.md) |

---

**All documentation verified against actual source code for 100% accuracy.** 