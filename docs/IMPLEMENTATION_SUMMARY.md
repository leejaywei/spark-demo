# Test Filtering Feature - Implementation Summary

## Problem Statement (Chinese)
加个功能，可以跑单个测试，或者单组测试，设计一下看怎么样合理

**Translation:** Add a feature to run a single test or a group of tests, and design how to make it reasonable/practical.

## Solution Overview

Implemented a flexible test filtering system that allows users to:
1. List all available tests
2. Run tests from a specific group
3. Run tests matching a case name pattern
4. Combine filters for precise test selection

## Implementation Details

### 1. Code Changes to `iceberg_it.py`

#### Added imports:
```python
import argparse  # For command-line argument parsing
```

#### Modified `Suite.run_all()` method:
- Added optional parameters: `group_filter` and `case_filter`
- Implemented filtering logic before running tests
- Added informative messages showing test count
- Maintained backward compatibility (no args = run all)

#### Added `list_all_tests()` function:
- Groups tests by category
- Displays test counts per group
- Shows total test and group counts

#### Enhanced `main()` function:
- Added argparse for CLI argument handling
- Implemented `--list`, `--group`, `--case`, `--db` options
- Added helpful examples in epilog
- Integrated filter logic with Suite.run_all()

### 2. Documentation Updates

#### README.md
- Added "Run Specific Tests" section
- Documented all command-line options
- Provided practical examples for each option

#### docs/test-filtering-examples.md
- Comprehensive examples with expected outputs
- Design rationale explanation
- Use cases and scenarios

### 3. Design Principles

1. **Minimal Changes**: Only modified necessary parts of the code
2. **Backward Compatible**: Running without arguments works exactly as before
3. **Flexible Matching**: Case filter supports partial matching for convenience
4. **Composable**: Filters can be combined for precise selection
5. **User-Friendly**: Clear feedback and helpful error messages
6. **Well-Documented**: Inline help, README, and detailed examples

## Usage Examples

### List all tests
```bash
python iceberg_it.py --list
```

### Run all tests in a group
```bash
python iceberg_it.py --group 10_ddl_core
```

### Run a specific test
```bash
python iceberg_it.py --case create_table_as_select_basic
```

### Run tests matching a pattern
```bash
python iceberg_it.py --case merge
# Runs: merge_into_basic, merge_with_matched_delete, merge_multiple_matched_clauses, merge_into_on_branch
```

### Combine filters
```bash
python iceberg_it.py --group 20_writes_sql_core --case insert
# Runs only insert-related tests in the 20_writes_sql_core group
```

### Use custom database
```bash
python iceberg_it.py --db my_test_db --group 10_ddl_core
```

## Testing

### Validation performed:
1. ✓ Python syntax validation (py_compile)
2. ✓ Filter logic unit tests (7 test scenarios)
3. ✓ Mock demonstration (shows feature working)
4. ✓ Documentation completeness check

### Test scenarios validated:
- No filters (returns all tests)
- Group filter only
- Case filter only  
- Combined filters
- No matches (empty result)
- Partial case name matching
- Multiple groups with same prefix

## Benefits

1. **Development Speed**: Run only relevant tests during development
2. **Debugging**: Quickly re-run failing tests without full suite
3. **CI/CD**: Enable parallel test execution by splitting groups
4. **Exploration**: Help users understand test organization
5. **Efficiency**: Save time by avoiding unnecessary test runs

## Files Modified

1. `iceberg_it.py` - Main test suite file
   - Added argparse support
   - Modified run_all() with filtering
   - Added list_all_tests() function
   - Enhanced main() with CLI parsing

2. `README.md` - Project documentation
   - Added "Run Specific Tests" section
   - Documented command-line options

## Files Created

1. `docs/test-filtering-examples.md` - Detailed examples and design rationale

## Backward Compatibility

✓ Running `python iceberg_it.py` without arguments works exactly as before
✓ All existing functionality preserved
✓ No breaking changes to test structure or methods

## Code Quality

✓ Follows existing code style and conventions
✓ Properly typed with Optional hints
✓ Clear docstrings and comments
✓ Informative user messages
✓ Graceful handling of edge cases

## Future Enhancements (Optional)

Possible improvements that could be added later:
- Regular expression support for case matching
- Test exclusion filters (--exclude-group, --exclude-case)
- JSON/XML output format for CI/CD integration
- Test tags for cross-cutting concerns
- Dry-run mode to show what would be executed
