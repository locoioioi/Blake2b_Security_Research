import hashlib
import psutil
import os
import csv
import argparse
import subprocess
from blake3 import blake3
from scipy.stats import ttest_ind
import pandas as pd
import time

# Base directories
data_dir = "code/data/resources"
default_results_dir = "results"

# Ensure base directories exist
os.makedirs(data_dir, exist_ok=True)
os.makedirs(default_results_dir, exist_ok=True)

# Generate deterministic binary datasets using fsutil
def generate_deterministic_files(size_mb):
    size_bytes = size_mb * 1024 * 1024  # Convert MB to bytes
    file_path = os.path.join(data_dir, f"dataset_{size_mb}MB.bin")

    if os.path.exists(file_path):
        return file_path

    try:
        subprocess.run(["fsutil", "file", "createnew", file_path, str(size_bytes)], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error creating dataset with fsutil: {e}")
        raise

    return file_path

# Measure resource usage with caching
def measure_resource_usage(algorithm, data_size_mb, iterations):
    # Generate deterministic binary file
    file_path = generate_deterministic_files(data_size_mb)

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
    cpu_usages = []
    for _ in range(iterations):
        start_time = time.time()
        with open(file_path, "rb") as file:
            while chunk := file.read(8192):  # Read file in 8KB chunks
                hash_function(chunk)
        elapsed_time = time.time() - start_time

        # Record CPU usage
        cpu_usage = psutil.cpu_percent(interval=None)
        cpu_usages.append(cpu_usage)

        # Check current memory usage
        current_memory_mb = process.memory_info().rss / (1024 * 1024)  # Memory in MB
        peak_memory_mb = max(peak_memory_mb, current_memory_mb)

    return cpu_usages, peak_memory_mb

# Test resource usage
def test_resource_usage(algorithms, data_sizes_mb, iterations):
    results = []
    for algo in algorithms:
        for size_mb in data_sizes_mb:
            cpu_usages, peak_memory = measure_resource_usage(algo, size_mb, iterations)
            for cpu in cpu_usages:
                results.append([algo, size_mb, cpu, peak_memory])
    return results

# Perform T-tests
def perform_t_tests(results_csv, output_folder):
    df = pd.read_csv(results_csv)
    data_sizes = df["Data Size (MB)"].unique()

    # Algorithm pairs to compare
    comparison_pairs = [("blake3", "sha256"), ("blake2s", "blake2b")]

    t_test_results = []

    for size in data_sizes:
        subset = df[df["Data Size (MB)"] == size]
        for algo1, algo2 in comparison_pairs:
            values_algo1 = subset[subset["Algorithm"] == algo1]["CPU (%)"].values
            values_algo2 = subset[subset["Algorithm"] == algo2]["CPU (%)"].values

            if len(values_algo1) > 1 and len(values_algo2) > 1:
                t_stat, p_value = ttest_ind(values_algo1, values_algo2, equal_var=False)
                t_test_results.append({
                    "Data Size (MB)": size,
                    "Algorithm 1": algo1,
                    "Algorithm 2": algo2,
                    "T-Statistic": round(t_stat, 4),
                    "P-Value": round(p_value, 6)
                })

    # Save T-test results to CSV
    t_test_output = os.path.join(output_folder, "hashing_resource_t_test_results.csv")
    pd.DataFrame(t_test_results).to_csv(t_test_output, index=False)
    print(f"T-test results saved to {t_test_output}")

# Calculate averages and save to another CSV
def calculate_averages(input_csv, output_folder):
    df = pd.read_csv(input_csv)

    avg_df = (
        df.groupby(["Algorithm", "Data Size (MB)"])
        .agg(Average_CPU_Usage=("CPU (%)", "mean"), Peak_Memory=("Peak Memory (MB)", "mean"))
        .reset_index()
    )

    avg_csv = os.path.join(output_folder, "hashing_resource_avg_results.csv")
    avg_df.to_csv(avg_csv, index=False)

# Main function
def main():
    print("Measuring resource usage with binary test data...")
    # Argument parser
    parser = argparse.ArgumentParser(description="Measure and analyze resource usage of hashing algorithms.")
    parser.add_argument("--output", type=str, required=True, help="Subdirectory in the results folder to save the results.")
    args = parser.parse_args()

    # Algorithms to test
    algorithms = ['md5', 'sha1', 'sha256', 'sha512', 'sha3_256', 'blake2s', 'blake2b', 'blake3']
    # Data sizes to test (MB)
    data_sizes_mb = [1, 2, 4, 8, 16, 32, 64, 128, 200, 512]
    # Fixed number of iterations
    iterations = 5

    # Perform tests
    results = test_resource_usage(algorithms, data_sizes_mb, iterations)

    # Write results to CSV
    results_dir = os.path.join(default_results_dir, args.output) + "/resource_usage"
    os.makedirs(results_dir, exist_ok=True)
    results_csv = os.path.join(results_dir, "hashing_resource_results.csv")

    with open(results_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Algorithm", "Data Size (MB)", "CPU (%)", "Peak Memory (MB)"])
        for result in results:
            writer.writerow(result)

    print(f"Resource results have been written to {results_csv}")

    # Perform T-tests on the results
    perform_t_tests(results_csv, results_dir)

    # Calculate averages and save to CSV
    calculate_averages(results_csv, results_dir)

if __name__ == "__main__":
    main()
