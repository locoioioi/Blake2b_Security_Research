import matplotlib.pyplot as plt
import csv
import argparse
import os

# Function to read results from a CSV file
def read_resource_results_from_csv(file_path):
    results = []
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            # Convert relevant fields to appropriate types
            results.append([row[0], int(row[1]), int(row[2]), float(row[3]), float(row[4])])
    return results

# Visualization of CPU Usage
def visualize_cpu_usage(results, output_dir):
    # Organize data for plotting
    data = {algo: {"sizes": [], "cpu": []} for algo in set(result[0] for result in results)}
    for algo, size_mb, _, cpu, _ in results:
        data[algo]["sizes"].append(size_mb)
        data[algo]["cpu"].append(cpu)
    
    # Plot each algorithm's CPU usage
    plt.figure(figsize=(12, 8))
    for algo in data.keys():
        plt.plot(data[algo]["sizes"], data[algo]["cpu"], marker='o', label=algo.upper())

    # Chart details
    plt.title("CPU Usage Across Data Sizes", fontsize=16)
    plt.xlabel("Data Size (MB)", fontsize=14)
    plt.ylabel("CPU Usage (%)", fontsize=14)
    plt.legend(title="Algorithm", fontsize=12)
    plt.grid(True)
    plt.tight_layout()

    # Save the image
    output_path = os.path.join(output_dir, "cpu_usage.png")
    plt.savefig(output_path)
    print(f"Saved: {output_path}")
    plt.close()

# Visualization of Peak Memory Usage
def visualize_memory_usage(results, output_dir):
    # Organize data for plotting
    data = {algo: {"sizes": [], "memory": []} for algo in set(result[0] for result in results)}
    for algo, size_mb, _, _, memory_kb in results:
        data[algo]["sizes"].append(size_mb)
        data[algo]["memory"].append(memory_kb / 1024)  # Convert KB to MB for readability
    
    # Plot each algorithm's memory usage
    plt.figure(figsize=(12, 8))
    for algo in data.keys():
        plt.plot(data[algo]["sizes"], data[algo]["memory"], marker='o', label=algo.upper())

    # Chart details
    plt.title("Peak Memory Usage Across Data Sizes", fontsize=16)
    plt.xlabel("Data Size (MB)", fontsize=14)
    plt.ylabel("Peak Memory Usage (MB)", fontsize=14)
    plt.legend(title="Algorithm", fontsize=12)
    plt.grid(True)
    plt.tight_layout()

    # Save the image
    output_path = os.path.join(output_dir, "memory_usage.png")
    plt.savefig(output_path)
    print(f"Saved: {output_path}")
    plt.close()

# Main function
def main():
    # Argument parser
    parser = argparse.ArgumentParser(description="Visualize resource usage from hashing results.")
    parser.add_argument("--folder", type=str, required=True, help="Subdirectory in the results folder to read the results.")
    args = parser.parse_args()

    # Construct the path to the results file
    results_dir = os.path.join("results", args.folder)
    results_file = os.path.join(results_dir, "hashing_resource_results.csv")
    visualization_dir = os.path.join("visualization", args.folder)

    # Ensure visualization directory exists
    os.makedirs(visualization_dir, exist_ok=True)

    if not os.path.exists(results_file):
        print(f"Error: Results file not found at {results_file}")
        return

    # Read results and visualize
    print(f"Reading results from: {results_file}")
    results = read_resource_results_from_csv(results_file)

    # Generate visualizations
    visualize_cpu_usage(results, visualization_dir)
    visualize_memory_usage(results, visualization_dir)

if __name__ == "__main__":
    main()
