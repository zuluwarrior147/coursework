# Test Suite for ML Coursework

This directory contains comprehensive tests for the data loading and processing utilities.

## 🚀 Quick Start

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_loader.py

# Generate coverage report
uv run pytest --cov-report=html
```

## 📋 Test Coverage

### ✅ 100% Coverage for `loader.py` (19 tests)

- **`TestDatasetLoader`** (10 tests) - HTTP downloads, extraction, file handling
- **`TestBatchProcessor`** (5 tests) - Column filtering, batch processing, custom filters
- **`TestLoadAllDatasets`** (3 tests) - Main function with multiple datasets
- **`TestIntegrationWorkflow`** (1 test) - End-to-end workflow testing

## 🔧 Key Features

### ✨ Clean Architecture
- **Shared fixtures** - Reduced code duplication
- **Proper imports** - No `sys.path` manipulation
- **Clear test names** - Descriptive and organized
- **Modular structure** - Easy to maintain and extend

### 🎯 Comprehensive Testing
- **Mock HTTP requests** - No network dependencies
- **Column filtering verification** - Ensures correct data output
- **Error handling** - Tests failure scenarios
- **File format support** - Both CSV and TSV processing
- **Integration testing** - Complete workflow validation

### 📊 Test Data
Uses realistic IMDB dataset samples:
- Movie identifiers (`tconst`)
- Title types and names
- Release years and genres
- Proper TSV formatting

## 📁 Project Structure

```
tests/
├── __init__.py
├── test_loader.py    # Main test suite
└── README.md         # This file

scripts/
├── __init__.py
├── loader.py         # Data loading utilities
└── cli.py           # Command-line interface
```

## ⚙️ Configuration

Tests are configured via `pyproject.toml`:
- Coverage reporting (HTML, XML, terminal)
- Test discovery patterns
- Warning filters
- JUnit XML output for CI/CD

## 🎯 Quality Metrics

- **19/19 tests passing** ✅
- **100% code coverage** for `loader.py`
- **Clean, maintainable code** with proper documentation
- **Fast execution** (~0.7s for full suite) 