# Logly Test Suite

This directory contains the test suite for the Logly monitoring system.

## Structure

```
tests/
├── unit/               # Unit tests for individual components
│   ├── test_*.py      # Test files (one per module)
│   └── ...
├── integration/        # Integration tests (if any)
└── README.md          # This file
```

## Running Tests

Run all tests:
```bash
pytest
```

Run with verbose output:
```bash
pytest -v
```

Run specific test file:
```bash
pytest tests/unit/test_issue_detector.py
```

Run specific test:
```bash
pytest tests/unit/test_issue_detector.py::TestErrorDetection::test_find_error_spikes_detected
```

## Recent Issues Fixed

### 1. Issue Detector Test Failures (2025-10-30)

#### Problem 1: `test_find_database_issues` - Incorrect Mock Return Type

**Issue**: The test was mocking `get_error_patterns()` to return a list, but the actual implementation expects a dictionary with `by_category` and `by_type` keys.

**Root Cause**: `SQLiteStore.get_error_patterns()` returns:
```python
{
    'by_type': [{'error_type': '...', 'count': N}, ...],
    'by_category': [{'error_category': '...', 'count': N}, ...]
}
```

**Solution**: Updated the mock to return the correct dictionary structure:
```python
mock_store.get_error_patterns.return_value = {
    "by_category": [{"error_category": "database", "count": 15}],
    "by_type": [{"error_type": "connection_timeout", "count": 5}]
}
```

**Location**: [test_issue_detector.py:396-417](tests/unit/test_issue_detector.py#L396-L417)

#### Problem 2: `test_error_query_*` - Wrong Method Being Mocked

**Issue**: Tests were mocking `get_error_patterns()` but `ErrorQuery` actually calls `get_error_traces()`.

**Root Cause**: The `ErrorQuery.all()` method calls:
```python
self.store.get_error_traces(start_time, end_time, category=self._category, limit=self._limit)
```

**Solution**: Changed all `ErrorQuery` tests to mock `get_error_traces` instead of `get_error_patterns`.

**Affected Tests**:
- `test_error_query_basic`
- `test_error_query_by_category`
- `test_error_query_database_errors`
- `test_error_query_resource_errors`
- `test_error_query_by_type`

**Location**: [test_query_builder.py:311-370](tests/unit/test_query_builder.py#L311-L370)

#### Problem 3: `test_find_error_spikes_detected` - Hour Bucketing Logic

**Issue**: The error spike detection was not working because:
1. The implementation wasn't sorting hour buckets chronologically (bug in implementation)
2. Test data was generating timestamps that crossed hour boundaries, causing spike data to be split across multiple buckets

**Root Cause Analysis**:

The spike detection algorithm groups errors by hour using integer division:
```python
hour_bucket = timestamp // 3600
```

When creating test data like `current_time - i * 100` for i in range(20), the span is 1900 seconds, which can cross hour boundaries depending on where `current_time` falls within its hour.

**Implementation Bug Fixed**:
The code was using `list(hourly_counts.values())` which doesn't preserve chronological order. Fixed by sorting hour keys first:
```python
# Before (WRONG - unordered)
counts = list(hourly_counts.values())

# After (CORRECT - chronologically sorted)
sorted_hours = sorted(hourly_counts.keys())
counts = [hourly_counts[h] for h in sorted_hours]
```

**Test Data Fixed**:
Changed to explicitly calculate hour buckets and place all errors within the same bucket:
```python
current_hour_bucket = current_time // 3600

# Normal baseline: 5 errors per hour for previous 3 hours
for h in range(1, 4):
    hour_bucket = current_hour_bucket - h
    base_time = hour_bucket * 3600 + 1800  # Middle of hour
    # ... create 5 errors ...

# Spike: 20 errors in current hour
base_time = current_hour_bucket * 3600 + 1800  # Middle of current hour
# ... create 20 errors ...
```

**Location**:
- Implementation fix: [issue_detector.py:588-592](logly/query/issue_detector.py#L588-L592)
- Test fix: [test_issue_detector.py:316-354](tests/unit/test_issue_detector.py#L316-L354)

## Best Practices for Writing Tests

### 1. Mock the Correct Methods
Always verify which methods are actually called by the code under test. Use debuggers or read the implementation carefully.

### 2. Match Data Structures
When mocking return values, ensure the structure matches what the actual implementation returns. Check:
- Is it a list or dict?
- What keys does the dict have?
- What fields do list items contain?

### 3. Time-Based Tests
When testing time-based logic with bucketing/windowing:
- **Explicitly calculate buckets** rather than assuming relative timestamps
- **Place test data in the middle of buckets** to avoid boundary issues
- **Verify bucket calculations** with a quick script before writing the test
- Remember: `timestamp // bucket_size` for integer division bucketing

Example:
```python
# Good: Explicit bucket calculation
current_hour_bucket = current_time // 3600
base_time = current_hour_bucket * 3600 + 1800  # Middle of hour

# Bad: Relative timestamps that might cross boundaries
base_time = current_time - 1800  # Might be in wrong bucket!
```

### 4. Testing Algorithms with State
For algorithms that track state across iterations (like spike detection):
- Ensure the algorithm processes data in the expected order
- Sort data if the algorithm assumes chronological order
- Test boundary conditions (e.g., exactly at threshold)

## Debugging Failed Tests

1. **Read the error message carefully** - It often tells you exactly what's wrong
2. **Check what's being mocked** - Verify the mock is for the right method
3. **Verify data structures** - Print the actual vs expected values
4. **Trace through the logic** - Use a Python script to manually verify the algorithm
5. **Check for ordering assumptions** - Many algorithms assume sorted data

## Test Coverage

Run with coverage:
```bash
pytest --cov=logly --cov-report=html
```

View coverage report:
```bash
open htmlcov/index.html
```
