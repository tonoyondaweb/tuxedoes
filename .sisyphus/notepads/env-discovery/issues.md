# Issues

## [TIMESTAMP] Task: N/A
*Initial notepad for env-discovery project*

## Issue Found During QA (Task F3)

### Date: 2026-03-23

### Bug in retry.py - Missing return statement
**Location**: `/Users/tonoy/projects/tuxedoes/src/discovery/utils/retry.py`

**Issue**: The `retry()` function was missing a `return decorator` statement at the end of the function definition. This caused a "TypeError: 'NoneType' object is not callable" error when trying to use the @retry decorator.

**Root Cause**: 
```python
def retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,)):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # retry logic
        return wrapper
    # MISSING: return decorator  <-- This was missing!
```

**Fix Applied**:
Added `return decorator` at line 88 of retry.py.

**Impact**: This bug prevented the entire extraction orchestrator from being imported, causing the CLI to fail immediately when trying to run any extraction command.

**Verification**: After fix, retry decorator works correctly and extraction command runs (though it fails on missing connection parameters, which is expected).


## Code Quality Review - Task F2

### Date: 2026-03-23 17:56:59

### Summary

**VERDICT: REJECT** - Multiple code quality issues found that must be addressed.

---

### LSP Diagnostics (3 Errors)

#### Error 1: Return Type Mismatch
**Location**: `src/discovery/extract/connection.py:76:19`
**Issue**: Type "SnowflakeConnection" from snowflake.connector.connection is not assignable to return type "SnowflakeConnection" (local class)
**Root Cause**: The `connect()` method returns `self._conn` instead of `self`, causing type confusion between the wrapper class and the internal connection object
**Impact**: Type safety violation, potential runtime issues

#### Error 2: Return Type Mismatch
**Location**: `src/discovery/extract/connection.py:109:19`
**Issue**: Same as Error 1
**Root Cause**: Same as Error 1
**Impact**: Type safety violation

#### Error 3: Unreachable Except Clause
**Location**: `src/discovery/extract/connection.py:113:15`
**Issue**: Except clause is unreachable because exception is already handled
**Root Cause**: `OperationalError` is a subclass of `DatabaseError`, so the except at line 113 will never be reached
**Impact**: Dead code, misleading error handling logic

---

### Code Quality Issues

#### Issue 1: Duplicate Exception Definitions
**Location**: `src/discovery/utils/retry.py` lines 14-48
**Issue**: Exception classes (`ExtractionError`, `ConfigValidationError`, `ConnectionError`, `PartialExtractionError`) are duplicated between `retry.py` and `errors.py`
**Violation**: DRY principle violated
**Correct Location**: All exception classes should be defined in `errors.py` only
**Impact**: Code maintenance nightmare, potential confusion, inconsistent exception behavior

#### Issue 2: Wrong Exception Class Imported
**Location**: `src/discovery/orchestrator.py:28`
**Issue**: Imports `ConnectionError` from `.extract.connection` (simple Exception subclass) instead of from `.utils.errors` (rich DiscoveryError subclass with context tracking)
**Impact**: Loss of context tracking capabilities, inconsistent error handling across codebase

#### Issue 3: Variable Name Mismatch
**Location**: `src/discovery/extract/connection.py:87-88`
**Issue**: List comprehension checks for 'private_key' but the variable is named `private_key_raw`
**Code**:
```python
missing = [k for k in ['account', 'user', 'warehouse', 'database', 'private_key']
          if not self._config.get(k) and not os.getenv(k)]
```
**Expected**: Should check for 'private_key' in config, but the actual parameter is 'private_key_raw'
**Impact**: Incorrect error message for missing private key

---

### AI-Slop Check

✅ **PASSED** - No AI-slop detected:
- No excessive comments (>30% comment ratio)
- No over-abstraction
- No generic variable names (data/result/item/temp)
- No bare excepts
- No unused imports in reviewed files
- No print statements in production code

---

### Build/Typecheck

✅ **PASSED** - Python compilation successful (`py_compile` exit code 0)
⚠️ **Mypy not available** - Could not run mypy for full type checking

---

### Test Results

❌ **FAILED** - Tests have failures:
- `test_extraction_orchestrator_connects_to_snowflake` - Mock assertion failure
- Import issue was previously fixed (missing `return decorator` in retry.py)

---

### Files Reviewed

✅ `src/discovery/utils/__init__.py` - Clean, proper imports, no AI-slop
❌ `src/discovery/extract/connection.py` - 3 LSP errors, variable name mismatch
❌ `src/discovery/utils/retry.py` - Duplicate exception definitions
❌ `src/discovery/orchestrator.py` - Wrong ConnectionError import

---

### Required Fixes (Blocking)

1. **Fix connection.py return statements**:
   - Line 76: Change `return self._conn` to `return self`
   - Line 109: Change `return self._conn` to `return self`

2. **Remove unreachable except clause**:
   - Line 113-115: Remove `except sf_errors.OperationalError` clause

3. **Remove duplicate exceptions from retry.py**:
   - Delete exception class definitions (lines 14-48) from `retry.py`
   - Ensure `retry.py` imports exceptions from `errors.py` if needed

4. **Fix ConnectionError import in orchestrator.py**:
   - Line 28: Remove `ConnectionError` from `.extract.connection` import
   - Import `ConnectionError` from `.utils.errors` instead

5. **Fix variable name in connection.py**:
   - Line 87-88: Update list comprehension to use correct parameter names

---

### Summary Statistics

- **LSP Errors**: 3
- **Test Failures**: 1
- **Code Quality Issues**: 4 (High priority)
- **AI-Slop**: 0
- **VERDICT**: REJECT

**Note**: Previous retry.py bug (missing `return decorator`) was fixed but left code quality issues unresolved.

