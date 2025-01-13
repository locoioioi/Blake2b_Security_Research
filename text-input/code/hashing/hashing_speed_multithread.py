import hashlib
import time
import os
import csv
import argparse
from threading import Thread, Lock
from queue import Queue
from blake3 import blake3
import subprocess
from scipy.stats import ttest_ind
import pandas as pd

MAX_ITERATIONS = 5
MAX_THREADS = 8
RUNS_PER_TEST = 5  # Number of runs for meaningful T-tests
CHUNK_SIZE = 64 * 1024  # 64KB for file reads

data_dir = "code/data/speed"
results_dir = "results"

os.makedirs(data_dir, exist_ok=True)
os.makedirs(results_dir, exist_ok=True)

lock = Lock()

# Generate deterministic datasets using fsutil
def generate_deterministic_files(total, size_mb):
    size_bytes = size_mb * 1024 * 1024  # Convert MB to Bytes
    file_path = os.path.join(data_dir, f"dataset_{size_mb}MB_{total}.bin")

    if os.path.exists(file_path):
        return file_path

    try:
        subprocess.run(["fsutil", "file", "createnew", file_path, str(size_bytes)], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error while creating dataset with fsutil: {e}")
        raise

    return file_path

# Warm-up function
def warm_up(file_path, hash_function):
    with open(file_path, "rb") as file:
        while chunk := file.read(CHUNK_SIZE):  # Read file in 64KB chunks
            hash_function(chunk)

# Measure hashing speed
def measure_hashing_speed(algorithm, data_size_mb):
    file_path = generate_deterministic_files(MAX_ITERATIONS, data_size_mb)

    if algorithm == "blake3":
        hash_function = lambda x: blake3(x).digest()
    else:
        hash_function = lambda x: hashlib.new(algorithm, x).digest()

    warm_up(file_path, hash_function)

    timings = []
    for _ in range(RUNS_PER_TEST):
        start_time = time.time()
        with open(file_path, "rb") as file:
            while chunk := file.read(CHUNK_SIZE):
                hash_function(chunk)
        end_time = time.time()
        timings.append((end_time - start_time) * 1e3)  # Convert seconds to milliseconds

    total_time = sum(timings)
    avg_time = total_time / RUNS_PER_TEST
    speed = (data_size_mb * MAX_ITERATIONS * RUNS_PER_TEST) / (total_time / 1000)  # MBps
    return timings, total_time, avg_time, speed

# Worker function for threading
def worker(queue, timing_results, summary_results):
    while not queue.empty():
        algo, size_mb = queue.get()
        try:
            timings, total_time, avg_time, speed = measure_hashing_speed(algo, size_mb)
            with lock:
                # Add individual timings for t-tests
                for timing in timings:
                    timing_results.append([algo, size_mb, timing])

                # Add summary metrics
                summary_results.append([algo, size_mb, MAX_ITERATIONS, total_time, avg_time, speed])
        except Exception as e:
            print(f"Error processing {algo} with {size_mb}MB: {e}")
        finally:
            queue.task_done()

# Multithreaded hashing test
def test_multithreading(algorithms, data_sizes_mb, num_threads=MAX_THREADS):
    timing_results = []
    summary_results = []
    queue = Queue()

    for algo in algorithms:
        for size_mb in data_sizes_mb:
            queue.put((algo, size_mb))

    threads = []
    for _ in range(num_threads):
        thread = Thread(target=worker, args=(queue, timing_results, summary_results))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    return timing_results, summary_results

# Perform T-tests for specified algorithm pairs
def perform_t_tests(timing_results_csv, output_folder):
    df = pd.read_csv(timing_results_csv)
    data_sizes = df["Data Size (MB)"].unique()

    comparison_pairs = [("blake3", "sha256"), ("blake2s", "blake2b")]
    t_test_results = []

    for size in data_sizes:
        subset = df[df["Data Size (MB)"] == size]
        for algo1, algo2 in comparison_pairs:
            timings_algo1 = subset[subset["Algorithm"] == algo1]["Timing (ms)"].values
            timings_algo2 = subset[subset["Algorithm"] == algo2]["Timing (ms)"].values

            if len(timings_algo1) > 1 and len(timings_algo2) > 1:
                t_stat, p_value = ttest_ind(timings_algo1, timings_algo2, equal_var=False)
                t_test_results.append({
                    "Data Size (MB)": size,
                    "Algorithm 1": algo1,
                    "Algorithm 2": algo2,
                    "T-Statistic": round(t_stat, 4),
                    "P-Value": round(p_value, 6)
                })

    t_test_output = os.path.join(output_folder, "hashing_t_multi_threads_test_results.csv")
    pd.DataFrame(t_test_results).to_csv(t_test_output, index=False)
    print(f"T-test results saved to {t_test_output}")

# Main function
def main():
    parser = argparse.ArgumentParser(description="Run multithreaded hashing speed test and save results to CSV.")
    parser.add_argument("--output", type=str, required=True, help="Output subdirectory under ./results/")
    args = parser.parse_args()

    output_folder = os.path.join(results_dir, args.output) + "/hashing"
    os.makedirs(output_folder, exist_ok=True)

    algorithms = ['blake3', 'blake2s', 'blake2b', 'sha256']
    data_sizes_mb = [1, 2, 4, 8, 16, 32, 64, 128, 200, 512]
    timing_results, summary_results = test_multithreading(algorithms, data_sizes_mb)

    # Save timing results for t-tests
    timing_results_csv = os.path.join(output_folder, "hashing_speed_multi_threads_timing.csv")
    pd.DataFrame(timing_results, columns=["Algorithm", "Data Size (MB)", "Timing (ms)"]).to_csv(timing_results_csv, index=False)
    print(f"Timing results saved to {timing_results_csv}")

    # Save summary metrics
    summary_results_csv = os.path.join(output_folder, "hashing_speed_multi_threads_summary.csv")
    pd.DataFrame(summary_results, columns=["Algorithm", "Data Size (MB)", "Iterations", "Total Time (ms)", "Avg Time (ms)", "Speed (MBps)"]).to_csv(summary_results_csv, index=False)
    print(f"Summary results saved to {summary_results_csv}")

    # Perform and save T-tests
    perform_t_tests(timing_results_csv, output_folder)

if __name__ == "__main__":
    main()
