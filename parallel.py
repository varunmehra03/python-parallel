
import os
import tempfile
import pandas as pd
import multiprocessing
from joblib import Parallel, delayed
def process_csv_in_chunks(step):
    try:
        # Read csv into DataFrame
        df = pd.read_csv("/Users/vmehra/Desktop/nodes.csv", usecols=["~id"])

        # Split DataFrame into chunks
        chunk_size = len(df) // 10
        old_chunks = [df[i: i + chunk_size] for i in range(0, df.shape[0], chunk_size)]

        chunks = []
        for chunk in old_chunks:
            chunks.append(chunk)

        global_dir = os.path.join(tempfile.gettempdir(), "global")
        os.makedirs(global_dir, exist_ok=True)
        #shared counter to track progress
        # Create a tuple of chunks and filenames

        with multiprocessing.Manager() as manager:
            processed_chunks = manager.Value('i', 0)
            processed_chunks_lock = manager.Lock()

            tasks = [
                (step, chunk, os.path.join(global_dir, f"chunk_{i + 1}.csv"), processed_chunks, processed_chunks_lock) for i, chunk in enumerate(chunks)
            ]

            Parallel(n_jobs=4)(
                delayed(process_and_save_chunk)(*p) for p in tasks
            )
            # Access the final value of the shared counter after parallel processing
            total_processed_items = processed_chunks.value
            print(f'Total processed items: {total_processed_items} / {len(chunks)}')

            print(f"Step {step}: Finished processing of the csv in chunks")
    except Exception as e:
        print(
            f"Step {step}: An error occurred during processing of the csv in chunks. {e}"
        )
        raise

def process_and_save_chunk(step, chunk, filename, processed_chunks, processed_chunks_lock):
    print(f"Step {step}: Processing chunk {filename}")
    gremlin_service = None
    try:

        # Turn chunk dataframe into dictionary with "~id" as key and {} as value
        processed_chunk = chunk.set_index("~id").T.to_dict("dict")
        total_items = len(processed_chunk)
        processed_items = 0

        # Line below was used for testing purposes
        # Modify processed_chunk and take only 10 first companies using head method
        processed_chunk = {k: processed_chunk[k] for k in list(processed_chunk)[:2]}
        total_items = len(processed_chunk)
        # Loop through the rows and calculate the risk
        for key in processed_chunk:
            # Get the company id
            company_id = key

            processed_chunk[key] = 0.0  # No suppliers and risk details, default to 0.0

            # Increment progress counter
            processed_items+=1
            # Log progress after every 1000 items processed
            if processed_items % 1 == 0:
                print(f"{filename}: Processed {processed_items}/{total_items}")

        print(f"Finished {processed_items}/{total_items} items in {filename}")
        # Increment the processed_chunks counter
        with processed_chunks_lock:
            processed_chunks.value += 1
            print(f"Finished Processing {processed_chunks.value} chunks")

    except Exception as e:
        print(f"Step {step}: An error occurred during processing of the chunk. {e}")
        raise

if __name__ == "__main__":
    process_csv_in_chunks(1)
