# Blake2b_Security_Research
# Test Merkle Tree and Hash Algorithms

This repository is designed to test the performance of different hash algorithms used in Merkle tree implementations, visualize their results, and analyze their efficiency in blockchain-related tasks. The repository includes scripts for running a server, simulating client operations, and generating visualization reports.

---

## Features

- Test and compare Merkle tree performance with various hash algorithms (`SHA256`, `Blake2b`, `MD5`, `SHA1`, `SHA3`).
- Generate reports with execution times for Merkle tree operations.
- Visualize and analyze results with grouped bar charts.
- **Upcoming**: Test Blake2 with text data.

---

## Prerequisites
Ensure you have the following installed:
- **Python 3.8 or higher**
- **Pip** for managing Python packages
---

## Installation
- Use `pip install` to install any library you miss
- Step 1: Run server
```
python test-data/server.py
```
- Step 2: Run client script
```
python test-data/client.py
```
- Step 3: Generate Visualization Reports
```
python visualization/main.py
```
