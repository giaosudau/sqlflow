# SQLFlow CI Trigger Guide

## Overview

SQLFlow's CI system is designed to be cost-effective and efficient:
<<<<<<< HEAD
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
=======
- **Unit tests**: Run automatically on every push/PR
- **Integration tests**: Triggered only by comments (expensive)
- **Performance tests**: Triggered only by comments (very expensive)

## How to Trigger Tests

### 1. Automatic Tests (Always Run)
These run on every push and pull request automatically:
- ✅ Unit tests
- ✅ Local integration tests (no external services)
- ✅ Code formatting and linting
- ✅ Basic examples

**Coverage**: ~72% comprehensive coverage

### 2. Comment-Triggered Tests (Manual Only)

#### Integration Tests
To run expensive integration tests with external services:

```
/test integration
```

**What this runs:**
- Phase 2 Integration Demo with Docker services
- External services integration tests (S3, PostgreSQL, etc.)
- Real database and file system tests
- Additional coverage reporting

**Expected duration**: 5-10 minutes

#### Performance Tests
To run performance benchmarks:

```
/test performance
```

**What this runs:**
>>>>>>> ee78c2e (Enhance CI workflow with comment-triggered testing - Added support for integration and performance tests triggered by issue comments, allowing collaborators to run specific tests on demand. Introduced a new guide for triggering tests and improved feedback mechanisms with status comments and reactions. All tests passing, ensuring robust CI functionality.)
- Performance benchmarks
- Memory usage tests
- Large dataset handling tests
- Performance regression detection
<<<<<<< HEAD
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

### Visual Guide

```
Pull Request → Conversation Tab → Add Comment
                                      ↓
┌─────────────────────────────────────────────────────────┐
│ Write                                         Preview   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│ /test integration                                       │
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

## When to Use Comment Triggers

### Use `/test integration` when:
=======

**Expected duration**: 10-15 minutes

#### All Tests
To run both integration and performance tests:

```
/test all
```

**Expected duration**: 15-25 minutes

## Comment Requirements

### Who Can Trigger Tests
Only repository collaborators can trigger tests via comments:
- ✅ Repository owners
- ✅ Collaborators with write access
- ✅ Team members
- ❌ External contributors (security measure)

### Comment Format
- Comments are **case-insensitive**
- Commands can be anywhere in the comment
- Multiple commands in one comment are supported

**Examples:**
```
/test integration
```

```
Hey @team, I think we should run integration tests on this PR.
/test integration
```

```
Let's run all tests to be sure:
/test all
```

## Visual Feedback

### Reactions
When you trigger tests, you'll see reactions on your comment:
- 🚀 Integration tests started
- 👀 Performance tests started

### Status Comments
The bot will automatically comment on the PR/issue with:
- ✅ Tests passed with link to run details
- ❌ Tests failed with link to run details

**Example:**
```
✅ Integration tests passed!

[View run details](https://github.com/your-repo/actions/runs/123456)
```

## Manual Dispatch (GitHub UI)

You can also trigger tests manually from the GitHub Actions tab:

1. Go to **Actions** tab in your repository
2. Select **CI** workflow
3. Click **Run workflow**
4. Choose test type:
   - `unit` (same as automatic)
   - `integration`
   - `performance` 
   - `all`

## Coverage Reporting

### Automatic Coverage (72%)
Every automatic run reports comprehensive coverage including:
- All unit tests
- Local integration tests (no external services)
- Uploaded to Codecov with main coverage report

### Additional Coverage
Comment-triggered tests add additional coverage:
- **Integration tests**: External services coverage (separate flag)
- **Performance tests**: Performance test coverage (separate flag)

All coverage reports are combined in Codecov with proper flags.

## Best Practices

### When to Use Comment Triggers

#### Use `/test integration` when:
>>>>>>> ee78c2e (Enhance CI workflow with comment-triggered testing - Added support for integration and performance tests triggered by issue comments, allowing collaborators to run specific tests on demand. Introduced a new guide for triggering tests and improved feedback mechanisms with status comments and reactions. All tests passing, ensuring robust CI functionality.)
- ✅ Adding new connector implementations
- ✅ Modifying database interaction code
- ✅ Changing Docker configurations
- ✅ Working on external service integrations
<<<<<<< HEAD
- ✅ Testing Enhanced S3 Connector features
- ✅ Validating resilient patterns
- ✅ Before merging feature PRs

### Use `/test performance` when:
=======
- ✅ Before merging critical features

#### Use `/test performance` when:
>>>>>>> ee78c2e (Enhance CI workflow with comment-triggered testing - Added support for integration and performance tests triggered by issue comments, allowing collaborators to run specific tests on demand. Introduced a new guide for triggering tests and improved feedback mechanisms with status comments and reactions. All tests passing, ensuring robust CI functionality.)
- ✅ Optimizing query performance
- ✅ Modifying large dataset handling
- ✅ Changing memory management
- ✅ Before releasing new versions
- ✅ After significant architecture changes

<<<<<<< HEAD
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
   /test integration
   /test performance
   ```
3. Verify all systems working correctly
4. Proceed with release if all tests pass

The new comment-triggered approach provides better visibility, control, and collaboration while maintaining security! 🚀 
=======
#### Use `/test all` when:
- ✅ Major releases
- ✅ Critical bug fixes
- ✅ Architectural changes
- ✅ Before deploying to production

### Cost Optimization Tips

1. **Use automatic tests first**: Let the fast tests pass before triggering expensive ones
2. **Target specific tests**: Use `/test integration` instead of `/test all` when possible
3. **Coordinate with team**: Avoid multiple people triggering tests simultaneously
4. **Use draft PRs**: Trigger expensive tests only when PR is ready for review

## Troubleshooting

### Tests Don't Start
- ✅ Check you're a repository collaborator
- ✅ Verify comment format (case-insensitive)
- ✅ Check GitHub Actions are enabled
- ✅ Ensure comment is on a PR or issue

### Tests Fail
- ✅ Check the run details link in the status comment
- ✅ Look for infrastructure issues (Docker, network)
- ✅ Verify external services are accessible
- ✅ Check for flaky test patterns

### No Coverage Report
- ✅ Verify Codecov token is configured
- ✅ Check if tests actually ran code paths
- ✅ Look for pytest coverage collection issues

## Migration Notes

### Previous Behavior
- All tests ran automatically (expensive)
- No cost control
- Long CI times for simple changes

### New Behavior
- Fast automatic tests for development velocity
- Expensive tests only when needed
- Better resource utilization
- Improved developer experience

---

## Quick Reference

| Command | Duration | Cost | When to Use |
|---------|----------|------|-------------|
| *Automatic* | 2-3 min | Low | Every change |
| `/test integration` | 5-10 min | Medium | External services |
| `/test performance` | 10-15 min | High | Performance critical |
| `/test all` | 15-25 min | Very High | Major releases | 
>>>>>>> ee78c2e (Enhance CI workflow with comment-triggered testing - Added support for integration and performance tests triggered by issue comments, allowing collaborators to run specific tests on demand. Introduced a new guide for triggering tests and improved feedback mechanisms with status comments and reactions. All tests passing, ensuring robust CI functionality.)
