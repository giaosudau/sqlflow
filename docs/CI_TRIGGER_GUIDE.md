# SQLFlow CI Trigger Guide

## Overview

SQLFlow's CI system is designed to be cost-effective and efficient:
<<<<<<< HEAD
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
=======
- **Unit tests**: Run automatically on every push/PR (fast, cheap)
- **Integration tests**: Manual trigger only (expensive, thorough)
- **Performance tests**: Manual trigger only (very expensive, comprehensive)
>>>>>>> 119a6b8 (Refactor CI workflow to remove comment-triggered tests - Updated the CI configuration to eliminate integration and performance tests triggered by comments, transitioning to a manual dispatch model for better control and reliability. Revised documentation to reflect the new testing approach and clarified test types and costs. All tests passing, ensuring robust CI functionality.)

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

### 2. Manual Tests (GitHub Actions Dispatch)

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
<<<<<<< HEAD
To run performance benchmarks:

```
/test performance
```

**What this runs:**
>>>>>>> ee78c2e (Enhance CI workflow with comment-triggered testing - Added support for integration and performance tests triggered by issue comments, allowing collaborators to run specific tests on demand. Introduced a new guide for triggering tests and improved feedback mechanisms with status comments and reactions. All tests passing, ensuring robust CI functionality.)
=======
**What runs:**
>>>>>>> 119a6b8 (Refactor CI workflow to remove comment-triggered tests - Updated the CI configuration to eliminate integration and performance tests triggered by comments, transitioning to a manual dispatch model for better control and reliability. Revised documentation to reflect the new testing approach and clarified test types and costs. All tests passing, ensuring robust CI functionality.)
- Performance benchmarks
- Memory usage tests
- Large dataset handling tests
- Performance regression detection
<<<<<<< HEAD
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
=======
- Stress testing
>>>>>>> 119a6b8 (Refactor CI workflow to remove comment-triggered tests - Updated the CI configuration to eliminate integration and performance tests triggered by comments, transitioning to a manual dispatch model for better control and reliability. Revised documentation to reflect the new testing approach and clarified test types and costs. All tests passing, ensuring robust CI functionality.)

**Duration**: 10-15 minutes
**Cost**: High (intensive testing)

#### All Tests
Runs both integration and performance tests together.

**Duration**: 15-25 minutes
**Cost**: Very High (comprehensive testing)

## How to Trigger Manual Tests

### Method: GitHub Actions UI

1. **Go to your repository on GitHub**
2. **Click the "Actions" tab**
3. **Select "CI" workflow from the left sidebar**
4. **Click "Run workflow" button (top right)**
5. **Choose test type from dropdown**:
   - `unit` - Same as automatic tests
   - `integration` - Run integration tests
   - `performance` - Run performance tests
   - `all` - Run both integration and performance tests
6. **Click "Run workflow" button**

### Visual Guide

```
GitHub Repository → Actions Tab → CI Workflow → Run workflow
                                                      ↓
┌─────────────────────────────────────────────────────────┐
│ Run workflow                                            │
├─────────────────────────────────────────────────────────┤
│ Use workflow from: [Branch: main        ▼]             │
│                                                         │
│ Type of tests to run:                                   │
│ [integration                            ▼]              │
│                                                         │
│ ┌─────────────────┐                                     │
│ │  Run workflow   │                                     │
│ └─────────────────┘                                     │
└─────────────────────────────────────────────────────────┘
```

### Test Type Options

| Option | Description | Duration | When to Use |
|--------|-------------|----------|-------------|
| `unit` | Standard unit tests + local integration | 2-3 min | Development, quick checks |
| `integration` | External services + Docker demos | 5-10 min | Feature testing, before merge |
| `performance` | Benchmarks + stress tests | 10-15 min | Performance validation |
| `all` | Complete test suite | 15-25 min | Release preparation, critical fixes |

## When to Use Manual Triggers

<<<<<<< HEAD
To trigger tests:
1. Go to your **Pull Request**
2. Add a comment with the trigger command
3. The tests will run on the PR's branch/commit

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
=======
### Use `integration` when:
>>>>>>> 119a6b8 (Refactor CI workflow to remove comment-triggered tests - Updated the CI configuration to eliminate integration and performance tests triggered by comments, transitioning to a manual dispatch model for better control and reliability. Revised documentation to reflect the new testing approach and clarified test types and costs. All tests passing, ensuring robust CI functionality.)
- ✅ Adding new connector implementations
- ✅ Modifying database interaction code
- ✅ Changing Docker configurations
- ✅ Working on external service integrations
<<<<<<< HEAD
<<<<<<< HEAD
- ✅ Testing Enhanced S3 Connector features
- ✅ Validating resilient patterns
- ✅ Before merging feature PRs

### Use `/test performance` when:
=======
- ✅ Before merging critical features

#### Use `/test performance` when:
>>>>>>> ee78c2e (Enhance CI workflow with comment-triggered testing - Added support for integration and performance tests triggered by issue comments, allowing collaborators to run specific tests on demand. Introduced a new guide for triggering tests and improved feedback mechanisms with status comments and reactions. All tests passing, ensuring robust CI functionality.)
=======
- ✅ Testing Enhanced S3 Connector features
- ✅ Validating resilient patterns
- ✅ Before merging feature PRs

### Use `performance` when:
>>>>>>> 119a6b8 (Refactor CI workflow to remove comment-triggered tests - Updated the CI configuration to eliminate integration and performance tests triggered by comments, transitioning to a manual dispatch model for better control and reliability. Revised documentation to reflect the new testing approach and clarified test types and costs. All tests passing, ensuring robust CI functionality.)
- ✅ Optimizing query performance
- ✅ Modifying large dataset handling
- ✅ Changing memory management
- ✅ Before releasing new versions
- ✅ After significant architecture changes

<<<<<<< HEAD
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
=======
### Use `all` when:
- ✅ Preparing major releases
- ✅ Critical bug fixes affecting core functionality
- ✅ Comprehensive validation before production deployment
- ✅ Weekly/monthly comprehensive testing
>>>>>>> 119a6b8 (Refactor CI workflow to remove comment-triggered tests - Updated the CI configuration to eliminate integration and performance tests triggered by comments, transitioning to a manual dispatch model for better control and reliability. Revised documentation to reflect the new testing approach and clarified test types and costs. All tests passing, ensuring robust CI functionality.)

## Cost Optimization

### Why Manual Triggers?
1. **Cost Control**: Expensive tests don't run automatically
2. **Resource Efficiency**: Run comprehensive tests only when needed
3. **Developer Velocity**: Fast feedback loop for development
4. **Targeted Testing**: Choose specific test types for specific changes

### Best Practices
1. **Run unit tests first**: Let automatic tests pass before running expensive ones
2. **Choose appropriate test type**: Don't run `all` for minor changes
3. **Coordinate with team**: Avoid running multiple expensive tests simultaneously
4. **Monitor resource usage**: Check GitHub Actions usage in repository settings

## Monitoring & Results

### Where to Find Results
1. **Actions Tab**: Real-time progress and logs
2. **PR Checks**: Status checks appear on pull requests
3. **Codecov Reports**: Coverage reports uploaded automatically
4. **Logs**: Detailed execution logs available in Actions tab

### Understanding Results

#### Success Indicators
- ✅ Green checkmark in Actions tab
- ✅ All jobs completed successfully
- ✅ Coverage reports uploaded
- ✅ Demo outputs generated correctly

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

## Migration Notes

### Previous Behavior (Removed)
- ❌ Comment-triggered tests (unreliable)
- ❌ Complex permission handling
- ❌ PR comment parsing
- ❌ Automatic reactions and responses

### New Behavior (Current)
- ✅ Simple manual triggers via GitHub UI
- ✅ Reliable workflow dispatch
- ✅ Clear test type selection
- ✅ Straightforward execution
- ✅ No comment dependencies

## Quick Reference

<<<<<<< HEAD
| Command | Duration | Cost | When to Use |
|---------|----------|------|-------------|
| *Automatic* | 2-3 min | Low | Every change |
| `/test integration` | 5-10 min | Medium | External services |
| `/test performance` | 10-15 min | High | Performance critical |
| `/test all` | 15-25 min | Very High | Major releases | 
>>>>>>> ee78c2e (Enhance CI workflow with comment-triggered testing - Added support for integration and performance tests triggered by issue comments, allowing collaborators to run specific tests on demand. Introduced a new guide for triggering tests and improved feedback mechanisms with status comments and reactions. All tests passing, ensuring robust CI functionality.)
=======
| Action | Method | Duration | Cost |
|--------|--------|----------|------|
| **Development Testing** | Automatic (push/PR) | 2-3 min | Low |
| **Feature Validation** | Manual → `integration` | 5-10 min | Medium |
| **Performance Check** | Manual → `performance` | 10-15 min | High |
| **Release Preparation** | Manual → `all` | 15-25 min | Very High |

---

## Example Workflows

### For Feature Development
1. Push changes → Automatic unit tests run
2. Review automatic test results
3. If passing → Manually trigger `integration` tests
4. If integration passes → Ready for review/merge

### For Performance Work
1. Push changes → Automatic tests run
2. Manually trigger `performance` tests
3. Review benchmark results
4. Optimize and repeat as needed

### For Release Preparation
1. All feature PRs merged and passing
2. Manually trigger `all` tests on main branch
3. Verify all systems working correctly
4. Proceed with release if all tests pass

The new manual-only approach is much more reliable and gives you complete control over when expensive tests run! 🚀 

## How to Trigger Tests by PR Comment

You can now trigger integration and performance tests directly from a pull request by commenting:

- `/test integration` — Runs integration tests
- `/test performance` — Runs performance tests
- `/test all` — Runs both integration and performance tests

**How to use:**
1. Open your pull request on GitHub
2. Add a comment with one of the above commands
3. The CI workflow will be triggered for the selected test type
4. Results will appear in the Actions tab and as PR checks

> This feature is powered by the [peter-evans/workflow-dispatch](https://github.com/peter-evans/workflow-dispatch) GitHub Action. 