# SQLFlow CI Trigger Guide

## Overview

SQLFlow's CI system is designed to be cost-effective and efficient:
- **Unit tests**: Run automatically on every push/PR (fast, cheap)
- **Integration tests**: Triggered via PR comments (expensive, thorough)
- **Performance tests**: Triggered via PR comments (very expensive, comprehensive)

## Test Types

### 1. Automatic Tests (Always Run)
These run automatically on every push and pull request:
- ✅ Unit tests
- ✅ Local integration tests (no external services)
- ✅ Code formatting and linting (black, isort, flake8)
- ✅ Basic example demos
- ✅ Coverage reporting (~72% comprehensive coverage)

**Duration**: 2-3 minutes
**Cost**: Low (standard GitHub Actions minutes)

### 2. Comment-Triggered Tests

#### Integration Tests
**What runs:**
- Phase 2 Integration Demo with Docker services
- External services integration tests (PostgreSQL, MinIO S3, Redis)
- Real database and file system tests
- Enhanced S3 Connector with cost management
- Resilient connector patterns testing
- Additional coverage reporting

**Duration**: 5-10 minutes
**Cost**: Medium (Docker + external services)

#### Performance Tests
**What runs:**
- Performance benchmarks
- Memory usage tests
- Large dataset handling tests
- Performance regression detection
- Stress testing

**Duration**: 10-15 minutes
**Cost**: High (intensive testing)

## How to Trigger Tests

### Method: PR Comments

1. **Integration Tests**
   ```
   /test integration
   ```

2. **Performance Tests**
   ```
   /test performance
   ```

3. **Run Both (Simple Commands)**
   ```
   test it
   run tests
   ```

### Visual Guide

```
Pull Request → Conversation Tab → Add Comment
                                      ↓
┌─────────────────────────────────────────────────────────┐
│ Write                                         Preview   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│ test it                                                 │
│                                                         │
│ ┌─────────────────┐                                     │
│ │    Comment      │                                     │
│ └─────────────────┘                                     │
└─────────────────────────────────────────────────────────┘
```

### Test Type Options

| Option | Command | Duration | When to Use |
|--------|---------|----------|-------------|
| `unit` | Automatic | 2-3 min | Development, quick checks |
| `integration` | `/test integration` | 5-10 min | Feature testing, before merge |
| `performance` | `/test performance` | 10-15 min | Performance validation |
| `both` | `test it` or `run tests` | 15-25 min | Comprehensive testing |

## When to Use Comment Triggers

### Use `/test integration` when:
- ✅ Adding new connector implementations
- ✅ Modifying database interaction code
- ✅ Changing Docker configurations
- ✅ Working on external service integrations
- ✅ Testing Enhanced S3 Connector features
- ✅ Validating resilient patterns
- ✅ Before merging feature PRs

### Use `/test performance` when:
- ✅ Optimizing query performance
- ✅ Modifying large dataset handling
- ✅ Changing memory management
- ✅ Before releasing new versions
- ✅ After significant architecture changes

### Use `test it` or `run tests` when:
- ✅ You want to run comprehensive testing (both integration + performance)
- ✅ You're not sure which specific test type you need
- ✅ You want maximum confidence before merging
- ✅ Simple, easy-to-remember command
- ✅ Perfect for final validation before release

## Cost Optimization

### Why Comment Triggers?
1. **Cost Control**: Expensive tests don't run automatically
2. **Resource Efficiency**: Run comprehensive tests only when needed
3. **Developer Velocity**: Fast feedback loop for development
4. **Targeted Testing**: Choose specific test types for specific changes
5. **Better Collaboration**: Test triggers visible in PR conversation
6. **Audit Trail**: Clear record of who triggered tests and when

### Best Practices
1. **Run unit tests first**: Let automatic tests pass before running expensive ones
2. **Choose appropriate test type**: Only trigger needed test types
3. **Coordinate with team**: Avoid running multiple expensive tests simultaneously
4. **Monitor resource usage**: Check GitHub Actions usage in repository settings
5. **Review before triggering**: Ensure code is ready for expensive tests
6. **Wait for completion**: Don't trigger new tests until current ones finish

## Monitoring & Results

### Where to Find Results
1. **Actions Tab**: Real-time progress and logs
2. **PR Checks**: Status checks appear on pull requests
3. **Codecov Reports**: Coverage reports uploaded automatically
4. **PR Comments**: Test trigger comments show 🚀 reaction when processed

### Understanding Results

#### Success Indicators
- ✅ Green checkmark in Actions tab
- ✅ All jobs completed successfully
- ✅ Coverage reports uploaded
- ✅ Demo outputs generated correctly
- ✅ 🚀 reaction on trigger comment

#### Failure Indicators
- ❌ Red X in Actions tab
- ❌ Job failures with error logs
- ❌ Missing output files
- ❌ Service startup failures

### Troubleshooting Failed Tests

1. **Check the logs**: Click on failed job for detailed logs
2. **Review error messages**: Look for specific failure points
3. **Verify service health**: Check if Docker services started correctly
4. **Check dependencies**: Ensure all required packages are installed
5. **Resource limits**: Verify if tests hit memory/time limits

## Security Considerations

### Comment Triggers
- ✅ Only works on pull requests (not issues)
- ✅ Limited permissions for security
- ✅ Clear audit trail in PR comments
- ✅ Visible test trigger history

## Quick Reference

| Action | Method | Duration | Cost |
|--------|--------|----------|------|
| **Development Testing** | Automatic (push/PR) | 2-3 min | Low |
| **Feature Validation** | Comment `/test integration` | 5-10 min | Medium |
| **Performance Check** | Comment `/test performance` | 10-15 min | High |

---

## Example Workflows

### For Feature Development
1. Push changes → Automatic unit tests run
2. Review automatic test results
3. If passing → Comment `/test integration`
4. If integration passes → Ready for review/merge

### For Performance Work
1. Push changes → Automatic tests run
2. Comment `/test performance`
3. Review benchmark results
4. Optimize and repeat as needed

### For Release Preparation
1. All feature PRs merged and passing
2. Run both test types on final PR:
   ```
   test it
   ```
   OR the traditional way:
   ```
   /test integration
   /test performance
   ```
3. Verify all systems working correctly
4. Proceed with release if all tests pass

### Quick Testing (Simple Commands)
1. Want to test everything quickly?
   ```
   test it
   ```
2. Alternative simple command:
   ```
   run tests
   ```
3. Both commands trigger comprehensive testing (integration + performance)

The new comment-triggered approach provides better visibility, control, and collaboration while maintaining security! 🚀 