#!/bin/bash

echo "Running E-commerce Data Pipeline Unit Tests"
echo "=============================================="

# Install test dependencies
echo "Installing test dependencies..."
pip install -r requirements-test.txt

# Run Lambda function tests
echo "Testing Lambda Functions..."
pytest tests/test_file_detector.py -v --tb=short
pytest tests/test_file_archiver.py -v --tb=short

# Run validation service tests
echo "Testing Validation Service..."
cd validation-service
pytest tests/ -v --tb=short
cd ..

# Run transformation service tests
echo "Testing Transformation Service..."
cd transformation-service
pytest tests/ -v --tb=short
cd ..

# Run integration tests
echo "Testing Integration..."
pytest tests/test_integration.py -v --tb=short

echo "All tests completed!"
