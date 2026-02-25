import pandas as pd
import dask.dataframe as dd
import time

filename = "5GB_fake_data.csv"
compressed_filename = "5GB_fake_data.gz"

# -------------------------------
# Pandas with chunksize
# -------------------------------
chunk_size = 100000
start_time = time.time()

total_rows = 0
for chunk in pd.read_csv(filename, chunksize=chunk_size):
    total_rows += len(chunk)

pandas_chunks_time = time.time() - start_time
print(f"Pandas with chunksize: {pandas_chunks_time:.2f} seconds, total rows: {total_rows}")

# -------------------------------
# Dask
# -------------------------------
start_time = time.time()

df_dask = dd.read_csv(filename)
total_rows_dask = df_dask.shape[0].compute()

dask_time = time.time() - start_time
print(f"Dask read: {dask_time:.2f} seconds, total rows: {total_rows_dask}")

# -------------------------------
# Compression
# -------------------------------
start_time = time.time()

df_compressed = pd.read_csv(compressed_filename, compression="gzip")
compressed_rows = len(df_compressed)

compression_time = time.time() - start_time
print(f"Compression: {compression_time:.2f} seconds, total rows: {compressed_rows}")

# -------------------------------
# Summary
# -------------------------------
print("\n--- Time Comparison ---")
print(f"Pandas (chunksize)    : {pandas_chunks_time:.2f} s")
print(f"Dask                  : {dask_time:.2f} s")
print(f"Compression           : {compression_time:.2f} s")