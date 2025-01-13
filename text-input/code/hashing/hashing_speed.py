import hashlib
import time
import os
import csv
import argparse
from blake3 import blake3
from scipy.stats import ttest_ind
import pandas as pd

# Configuration
MAX_ITERATIONS = 5
RUNS_PER_TEST = 5  # Number of runs for averaging and statistical testing
CHUNK_SIZE = 64 * 1024  # 64KB
data_dir = "code/data/speed"
results_dir = "results"

os.makedirs(data_dir, exist_ok=True)
os.makedirs(results_dir, exist_ok=True)

# Generate deterministic datasets using fsutil
def generate_deterministic_files(total, size_mb):
    size_bytes = size_mb * 1024 * 1024  # Convert MB to Bytes
    file_path = os.path.join(data_dir, f"dataset_{size_mb}MB_{total}.bin")

    # Check if the file already exists
    if os.path.exists(file_path):
        return file_path

    # Use fsutil to create the file
    os.system(f"fsutil file createnew {file_path} {size_bytes}")
    return file_path

# Warm-up function
def warm_up(file_path, hash_function):
    with open(file_path, "rb") as file:
        while chunk := file.read(CHUNK_SIZE):  # Read file in 64KB chunks
            hash_function(chunk)

# Measure hashing speed
def measure_hashing_speed(algorithm, data_size_mb, iterations):
    file_path = generate_deterministic_files(iterations, data_size_mb)

    if algorithm == "blake3":
        hash_function = lambda x: blake3(x).digest()
    else:
        hash_function = lambda x: hashlib.new(algorithm, x).digest()

    # Warm-up phase
    warm_up(file_path, hash_function)

    timings = []
    for _ in range(RUNS_PER_TEST):
        start_time = time.time()
        with open(file_path, "rb") as file:
            while chunk := file.read(CHUNK_SIZE):  # Read file in 64KB chunks
                hash_function(chunk)
        end_time = time.time()
        timings.append((end_time - start_time) * 1e3)  # Convert to milliseconds

    total_time = sum(timings)
    avg_time = total_time / RUNS_PER_TEST
    speed = (data_size_mb * iterations * RUNS_PER_TEST) / (total_time / 1000)  # MBps
    return timings, total_time, avg_time, speed

# Perform single-threaded test
def test_singlethread(algorithms, data_sizes_mb, iterations, output_folder):
    timing_results = []
    summary_results = []

    os.makedirs(output_folder, exist_ok=True)

    for algo in algorithms:
        for size_mb in data_sizes_mb:
            try:
                timings, total_time, avg_time, speed = measure_hashing_speed(algo, size_mb, iterations)

                # Add raw timings
                for timing in timings:
                    timing_results.append([algo, size_mb, timing])

                # Add summary metrics
                summary_results.append([algo, size_mb, iterations, total_time, avg_time, speed])
            except Exception as e:
                print(f"Error during test for {algo} with {size_mb}MB: {e}")

    # Save timing results to a CSV file
    timing_csv = os.path.join(output_folder, "hashing_speed_single_thread_timing.csv")
    pd.DataFrame(timing_results, columns=["Algorithm", "Data Size (MB)", "Timing (ms)"]).to_csv(timing_csv, index=False)
    print(f"Timing results saved to {timing_csv}")

    # Save summary results to a CSV file
    summary_csv = os.path.join(output_folder, "hashing_speed_single_thread_summary.csv")
    pd.DataFrame(summary_results, columns=["Algorithm", "Data Size (MB)", "Iterations", "Total Time (ms)", "Avg Time (ms)", "Speed (MBps)"]).to_csv(summary_csv, index=False)
    print(f"Summary results saved to {summary_csv}")

# Perform T-tests
def perform_t_tests(timing_csv, output_folder):
    df = pd.read_csv(timing_csv)
    data_sizes = df["Data Size (MB)"].unique()

    # Specify algorithm pairs for comparison
    comparison_pairs = [("blake3", "sha256"), ("blake2s", "blake2b")]

    t_test_results = []
    for size in data_sizes:
        subset = df[df["Data Size (MB)"] == size]
        for algo1, algo2 in comparison_pairs:
            times_algo1 = subset[subset["Algorithm"] == algo1]["Timing (ms)"].values
            times_algo2 = subset[subset["Algorithm"] == algo2]["Timing (ms)"].values

            if len(times_algo1) > 1 and len(times_algo2) > 1:
                t_stat, p_value = ttest_ind(times_algo1, times_algo2, equal_var=False)
                t_test_results.append({
                    "Data Size (MB)": size,
                    "Algorithm 1": algo1,
                    "Algorithm 2": algo2,
                    "T-Statistic": round(t_stat, 4),
                    "P-Value": round(p_value, 6)
                })

    # Save T-test results to CSV
    t_test_output = os.path.join(output_folder, "hashing_t_test_single_thread_results.csv")
    pd.DataFrame(t_test_results).to_csv(t_test_output, index=False)
    print(f"T-test results saved to {t_test_output}")

# Main function
def main():
    parser = argparse.ArgumentParser(description="Run single-threaded hashing speed test and save results to CSV.")
    parser.add_argument("--output", type=str, required=True, help="Output subdirectory under ./results/")
    args = parser.parse_args()

    output_folder = os.path.join(results_dir, args.output) + "/hashing"
    os.makedirs(output_folder, exist_ok=True)

    algorithms = ['md5', 'sha1', 'sha256', 'sha512', 'sha3_256', 'blake2s', 'blake2b', 'blake3']
    data_sizes_mb = [1, 2, 4, 8, 16, 32, 64, 128, 200, 512]
    iterations = MAX_ITERATIONS

    print("Running single-threaded hashing test...")
    test_singlethread(algorithms, data_sizes_mb, iterations, output_folder)

    # Perform T-tests
    timing_csv = os.path.join(output_folder, "hashing_speed_single_thread_timing.csv")
    perform_t_tests(timing_csv, output_folder)

if __name__ == "__main__":
    main()
