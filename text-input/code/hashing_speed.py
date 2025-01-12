import hashlib
import time
import os
import csv
import argparse
import statistics
from threading import Thread, Lock
from queue import Queue
from prettytable import PrettyTable
from blake3 import blake3
from scipy.stats import ttest_ind

MAX_ITERATIONS = 5
MAX_THREADS = 4

# Directories for data and results
data_dir = "code/data/speed"
default_results_dir = "results"

# Ensure directories exist
os.makedirs(data_dir, exist_ok=True)
os.makedirs(default_results_dir, exist_ok=True)

# Lock for thread-safe operations
lock = Lock()

# Generate deterministic datasets for testing
def generate_deterministic_strings(total, size_mb, reuse=True):
    size_bytes = size_mb * 1024 * 1024  # Convert MB to Bytes
    dataset_file = os.path.join(data_dir, f"dataset_{size_mb}MB_{total}.txt")

    if reuse and os.path.exists(dataset_file):
        with open(dataset_file, "r") as file:
            return [line.strip() for line in file.readlines()]

    data = [("a" * size_bytes) for _ in range(total)]
    with open(dataset_file, "w") as file:
        file.writelines(f"{item}\n" for item in data)
    return data

# Worker function for threading
def worker(queue, results):
    while not queue.empty():
        algo, size_mb, iterations = queue.get()
        try:
            # Measure hashing speed
            total_time, avg_time, mbps = measure_hashing_speed(algo, size_mb, iterations)
            with lock:
                results.append([algo, size_mb, iterations, total_time, avg_time, mbps])
        finally:
            queue.task_done()

# Measure hashing speed
def measure_hashing_speed(algorithm, data_size_mb, iterations):
    # Generate deterministic strings
    data = generate_deterministic_strings(iterations, data_size_mb)

    # Select the hashing algorithm
    if algorithm == "blake3":
        hash_function = lambda x: blake3(x).digest()
    else:
        hash_function = lambda x: hashlib.new(algorithm, x).digest()

    # Measure time to hash all data
    start_time = time.time()
    for s in data:
        hash_function(s.encode('utf-8'))
    end_time = time.time()

    # Calculate metrics
    total_time_ms = (end_time - start_time) * 1e3  # Convert seconds to milliseconds
    avg_time_per_hash = total_time_ms / iterations  # Time per hash in milliseconds
    mbps = (data_size_mb * iterations) / (end_time - start_time)  # Speed in megabytes per second
    return total_time_ms, avg_time_per_hash, mbps

# Test hashing algorithms with multithreading
def test_hashing_algorithms(algorithms, data_sizes_mb, iterations, num_threads=4):
    results = []
    threads = []
    queue = Queue()

    # Enqueue tasks
    for algo in algorithms:
        for size_mb in data_sizes_mb:
            queue.put((algo, size_mb, iterations))

    # Start threads
    for _ in range(num_threads):
        thread = Thread(target=worker, args=(queue, results))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    return results

# Perform statistical analysis
def perform_statistical_analysis(results):
    stats = []
    algorithms = set(r[0] for r in results)
    for algo in algorithms:
        algo_results = [r for r in results if r[0] == algo]
        speeds = [r[5] for r in algo_results]  # Extract speeds (MBps)
        stats.append({
            "Algorithm": algo,
            "Mean Speed (MBps)": statistics.mean(speeds),
            "StdDev Speed (MBps)": statistics.stdev(speeds) if len(speeds) > 1 else 0,
            "Min Speed (MBps)": min(speeds),
            "Max Speed (MBps)": max(speeds),
            "50th Percentile (MBps)": statistics.median(speeds),
        })
    return stats

# Perform pairwise t-tests
def perform_t_tests(results):
    algorithms = set(r[0] for r in results)
    algo_speeds = {algo: [r[5] for r in results if r[0] == algo] for algo in algorithms}
    comparisons = []
    algos = list(algorithms)

    for i in range(len(algos)):
        for j in range(i + 1, len(algos)):
            algo1, algo2 = algos[i], algos[j]
            t_stat, p_value = ttest_ind(algo_speeds[algo1], algo_speeds[algo2], equal_var=False)
            comparisons.append((algo1, algo2, t_stat, p_value))
    return comparisons

# Main function
def main():
    print("Measuring hashing speed with multithreading...")
    # Argument parser
    parser = argparse.ArgumentParser(description="Measure and analyze hashing speed with multithreading.")
    parser.add_argument("--output", type=str, required=True, help="Subdirectory in the results folder to save the results.")
    args = parser.parse_args()

    # Algorithms to test
    algorithms = ['md5', 'sha1', 'sha256', 'sha512', 'sha3_256', 'blake2s', 'blake2b', 'blake3']
    # Data sizes to test (MB)
    data_sizes_mb = [1, 2, 4, 8, 16, 32]
    # Fixed number of iterations
    iterations = MAX_ITERATIONS

    # Prepare PrettyTable for terminal output
    table = PrettyTable()
    table.field_names = ["Algorithm", "Data Size (MB)", "Iterations", "Total Time (ms)", "Avg Time (ms)", "Speed (MBps)"]

    # Perform tests and record results
    results = test_hashing_algorithms(algorithms, data_sizes_mb, iterations, num_threads=MAX_THREADS)
    for result in results:
        table.add_row([result[0], result[1], result[2], f"{result[3]:.3f}", f"{result[4]:.3f}", f"{result[5]:.3f}"])

    # Perform statistical analysis
    stats = perform_statistical_analysis(results)
    t_test_results = perform_t_tests(results)

    # Write results to CSV
    results_dir = os.path.join(default_results_dir, args.output)
    os.makedirs(results_dir, exist_ok=True)
    results_csv = os.path.join(results_dir, "hashing_speed_results.csv")
    stats_csv = os.path.join(results_dir, "hashing_speed_statistics.csv")
    t_tests_csv = os.path.join(results_dir, "hashing_t_tests.csv")

    with open(results_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Algorithm", "Data Size (MB)", "Iterations", "Total Time (ms)", "Avg Time (ms)", "Speed (MBps)"])
        for result in results:
            writer.writerow(result)

    with open(stats_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Algorithm", "Mean Speed (MBps)", "StdDev Speed (MBps)", "Min Speed (MBps)", "Max Speed (MBps)", "50th Percentile (MBps)"])
        for stat in stats:
            writer.writerow([stat["Algorithm"], f"{stat['Mean Speed (MBps)']:.3f}", f"{stat['StdDev Speed (MBps)']:.3f}",
                             f"{stat['Min Speed (MBps)']:.3f}", f"{stat['Max Speed (MBps)']:.3f}",
                             f"{stat['50th Percentile (MBps)']:.3f}"])

    with open(t_tests_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Algorithm 1", "Algorithm 2", "T-Statistic", "P-Value"])
        for comparison in t_test_results:
            writer.writerow([comparison[0], comparison[1], f"{comparison[2]:.4f}", f"{comparison[3]:.4f}"])

    # Output summary
    print(f"Results have been written to {results_csv}")
    print(f"Statistical analysis results have been written to {stats_csv}")
    print(f"T-Test results have been written to {t_tests_csv}")
    print(table)

if __name__ == "__main__":
    main()
