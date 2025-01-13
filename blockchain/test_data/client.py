import requests
import time
import os
from config import sender_id, recipient_id, port, tx_endpoint, mining_endpoint, chain_length, tx_amount

# Define the array of hash algorithms
hash_names = ["blake3","blake2b", "sha256", "blake2s", "sha512"]

def create_rounds(hash_name):
    """
    Create configuration for 9 rounds dynamically based on the hash algorithm.
    """
    return [
        {"puzzle": 2, "tx_per_block": 5, "results_file": f"test_data/results/{hash_name}/round1.txt"},
        {"puzzle": 2, "tx_per_block": 10, "results_file": f"test_data/results/{hash_name}/round2.txt"},
        {"puzzle": 2, "tx_per_block": 15, "results_file": f"test_data/results/{hash_name}/round3.txt"},
        {"puzzle": 4, "tx_per_block": 5, "results_file": f"test_data/results/{hash_name}/round4.txt"},
        {"puzzle": 4, "tx_per_block": 10, "results_file": f"test_data/results/{hash_name}/round5.txt"},
        {"puzzle": 4, "tx_per_block": 15, "results_file": f"test_data/results/{hash_name}/round6.txt"},
        {"puzzle": 6, "tx_per_block": 5, "results_file": f"test_data/results/{hash_name}/round7.txt"},
        {"puzzle": 6, "tx_per_block": 10, "results_file": f"test_data/results/{hash_name}/round8.txt"},
        {"puzzle": 6, "tx_per_block": 15, "results_file": f"test_data/results/{hash_name}/round9.txt"},
    ]

def clear_prev_result(directory: str): 
    """
    Clears all files in the specified results directory.

    Args:
        directory (str): Path to the directory containing results files.

    Returns:
        str: Message indicating whether the directory was cleared or did not exist.
    """
    try:
        if os.path.exists(directory):
            # Remove all files in the directory
            for file_name in os.listdir(directory):
                file_path = os.path.join(directory, file_name)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            return f"All previous results in '{directory}' have been cleared."
        else:
            return f"Results directory '{directory}' does not exist."
    except Exception as e:
        return f"Error clearing results directory: {e}"
def run_round(round_number, puzzle, tx_per_block, results_file):
    """
    Simulate mining and transaction processing for a single round.

    Args:
        round_number (int): Current round number.
        puzzle (int): Puzzle difficulty.
        tx_per_block (int): Number of transactions per block.
        results_file (str): Path to save results for this round.
    """
    print(f"Running Round {round_number}")
    print(f"Puzzle difficulty: {puzzle}, Transactions per block: {tx_per_block}")
    print(f"Results will be saved to {results_file}")
    print("-------------------------------------------------------------------")

    # Ensure results directory exists
    results_dir = os.path.dirname(results_file)
    os.makedirs(results_dir, exist_ok=True)

    # Mining and transaction simulation
    for block in range(chain_length):  # Iterate through blocks in the chain
        for tx in range(tx_per_block):  # Simulate transactions per block
            data = {'sender': sender_id, 'recipient': recipient_id, 'amount': tx_amount}
            try:
                res = requests.post(f'http://localhost:{port}{tx_endpoint}', json=data)
                res.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error during transaction: {e}")

        try:
            # Perform mining
            res = requests.get(f'http://localhost:{port}{mining_endpoint}')
            res.raise_for_status()
            response_data = res.json()

            # Extract mining time
            time_took = response_data.get('time took(ns)')
            if time_took is None:
                print(f"Warning: 'time took(ns)' key not found in response: {response_data}")
                time_took = "N/A"

            # Write mining time to the results file
            with open(results_file, "a") as file:
                file.write(f"{time_took}\n")

        except requests.exceptions.RequestException as e:
            print(f"Error during mining: {e}")


if __name__ == "__main__":
    # Iterate over all hash algorithms
    for hash_name in hash_names:
        print(f"Processing hash: {hash_name}")
        rounds = create_rounds(hash_name)

        # Clear all previous results for the current hash type
        clear_message = clear_prev_result(f"test_data/results/{hash_name}")
        print(clear_message)
        
        # Run all 9 rounds for the current hash algorithm
        for i, round_config in enumerate(rounds, start=1):
            run_round(i, round_config["puzzle"], round_config["tx_per_block"], round_config["results_file"])