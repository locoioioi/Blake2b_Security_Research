import os
import pandas as pd
import matplotlib.pyplot as plt
import argparse

# Argument parser
parser = argparse.ArgumentParser(description="Visualize hashing performance metrics.")
parser.add_argument("--folder", type=str, required=True, help="Folder name inside results and visualization directories.")
args = parser.parse_args()

# Directories for results and output
results_dir = os.path.join("results", args.folder)
output_dir = os.path.join("visualization", args.folder)

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Load CSV data into pandas DataFrame
def load_csv_data(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    return pd.read_csv(file_path)

# Visualization function
def visualize_metric(df, metric, ylabel, title, filename):
    plt.figure(figsize=(12, 6))
    algorithms = df["Algorithm"].unique()

    for algo in algorithms:
        subset = df[df["Algorithm"] == algo]
        plt.plot(subset["Data Size (MB)"], subset[metric], marker='o', label=algo.upper())

    # Add labels, title, and legend
    plt.title(title, fontsize=16)
    plt.xlabel("Data Size (MB)", fontsize=14)
    plt.ylabel(ylabel, fontsize=14)
    plt.legend(title="Algorithm", fontsize=12)
    plt.grid(True)
    plt.tight_layout()

    # Save the plot
    output_path = os.path.join(output_dir, filename)
    plt.savefig(output_path)
    print(f"Saved: {output_path}")
    plt.close()

# Main function
def main():
    # Load results
    results_file = os.path.join(results_dir, "hashing_speed_results.csv")

    # Read the main results CSV
    results_df = load_csv_data(results_file)

    # Visualize average hashing time
    visualize_metric(
        results_df,
        metric="Avg Time (ms)",
        ylabel="Average Time Per Hash (ms)",
        title="Hashing Time Per Algorithm",
        filename="avg_time_per_hash.png"
    )

    # Visualize hashing speed
    visualize_metric(
        results_df,
        metric="Speed (MBps)",
        ylabel="Speed (MBps)",
        title="Hashing Speed Per Algorithm",
        filename="hashing_speed.png"
    )

    # Visualize total time
    visualize_metric(
        results_df,
        metric="Total Time (ms)",
        ylabel="Total Time (ms)",
        title="Total Hashing Time Per Algorithm",
        filename="total_time.png"
    )

if __name__ == "__main__":
    main()
