# Blake2b_Security_Research

## Features

- Test and compare Merkle tree performance with various hash algorithms (`SHA256`, `Blake2b`, `Blake3`, `Blake2s`, `SHA512`).
- Visualize and analyze results with grouped bar charts.

---

## Prerequisites

Ensure you have the following installed:

- **Python 3.8 or higher**
- **Pip** for managing Python packages

---

## Run Blockchain Test

- Step 1: Install necessary libraries.

```
pip install -r requirement.txt
```

- Step 2: Access blockchain directory.

```
cd blockchain/
```

- Step 3: Run server

```
python test_data/server.py
```

- Step 4: Run client script

```
python test_data/client.py
```

- Step 5: Generate Visualization Reports

For Windows

```
python visualization/main.py --output Windows
```

For MacOS

```
python visualization/main.py --output MacOS
```

For Linux

```
python visualization/main.py --output Linux
```
