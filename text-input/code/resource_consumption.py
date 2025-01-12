import hashlib
import psutil
import os
import csv
import argparse
import statistics
from scipy.stats import ttest_ind
from blake3 import blake3
from prettytable import PrettyTable
import json

# Base directories
data_dir = "code/data/resources"
default_results_dir = "results"
cache_file = "resource_usage_cache.json"

# Ensure base directories exist
os.makedirs(data_dir, exist_ok=True)
os.makedirs(default_results_dir, exist_ok=True)

# Load or initialize cache
if os.path.exists(cache_file):
    with open(cache_file, "r") as f:
        cache = json.load(f)
else:
    cache = {}

# Generate deterministic datasets for testing
def generate_deterministic_datasets(total, size_mb, reuse=True):
    size_bytes = size_mb * 1024 * 1024  # Convert MB to Bytes
    dataset_file = os.path.join(data_dir, f"dataset_{size_mb}MB_{total}.txt")

    if reuse and os.path.exists(dataset_file):
        with open(dataset_file, "r") as file:
            return [line.strip() for line in file.readlines()]

    data = [("a" * size_bytes) for _ in range(total)]
    with open(dataset_file, "w") as file:
        file.writelines(f"{item}\n" for item in data)
    return data

# Measure resource usage with caching
def measure_resource_usage(algorithm, data_size_mb, iterations):
    # Cache key
    cache_key = f"{algorithm}-{data_size_mb}-{iterations}"
    if cache_key in cache:
        return cache[cache_key]

    # Generate deterministic datasets
    data = generate_deterministic_datasets(iterations, data_size_mb)

    # Select the hashing algorithm
    if algorithm == "blake3":
        hash_function = lambda x: blake3(x).digest()
    else:
        hash_function = lambda x: hashlib.new(algorithm, x).digest()

    # Get the current process
    process = psutil.Process(os.getpid())

    # Track peak memory usage
    peak_memory_mb = 0

    # Perform hashing and monitor memory
    for s in data:
        hash_function(s.encode('utf-8'))
        # Check current memory usage
        current_memory_mb = process.memory_info().rss / (1024 * 1024)  # Memory in MB
        peak_memory_mb = max(peak_memory_mb, current_memory_mb)

    # Calculate average CPU usage
    avg_cpu_usage = psutil.cpu_percent(interval=None)

    # Save result to cache
    cache[cache_key] = (avg_cpu_usage, peak_memory_mb)
    return avg_cpu_usage, peak_memory_mb

# Test resource usage
def test_resource_usage(algorithms, data_sizes_mb, iterations):
    results = []
    for algo in algorithms:
        for size_mb in data_sizes_mb:
            avg_cpu, peak_memory = measure_resource_usage(algo, size_mb, iterations)
            results.append([algo, size_mb, iterations, avg_cpu, peak_memory])
    return results

# Save cache to file
def save_cache():
    with open(cache_file, "w") as f:
        json.dump(cache, f)

# Main function
def main():
    print("Measuring resource usage with caching...")
    # Argument parser
    parser = argparse.ArgumentParser(description="Measure and analyze resource usage of hashing algorithms.")
    parser.add_argument("--output", type=str, required=True, help="Subdirectory in the results folder to save the results.")
    args = parser.parse_args()

    # Algorithms to test
    algorithms = ['md5', 'sha1', 'sha256', 'sha512', 'sha3_256', 'blake2s', 'blake2b', 'blake3']
    # Data sizes to test (MB)
    data_sizes_mb = [1, 2, 4, 8, 16]
    # Fixed number of iterations
    iterations = 5

    # Prepare PrettyTable for terminal output
    table = PrettyTable()
    table.field_names = ["Algorithm", "Data Size (MB)", "Iterations", "CPU (%)", "Peak Memory (MB)"]

    # Perform tests
    results = test_resource_usage(algorithms, data_sizes_mb, iterations)
    for result in results:
        table.add_row([result[0], result[1], result[2], f"{result[3]:.2f}", f"{result[4]:.2f}"])

    # Write results to CSV
    results_dir = os.path.join(default_results_dir, args.output)
    os.makedirs(results_dir, exist_ok=True)
    results_csv = os.path.join(results_dir, "hashing_resource_results.csv")

    with open(results_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Algorithm", "Data Size (MB)", "Iterations", "CPU (%)", "Peak Memory (MB)"])
        for result in results:
            writer.writerow(result)

    # Output summary
    print(f"Resource results have been written to {results_csv}")
    print(table)

if __name__ == "__main__":
    main()
