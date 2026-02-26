import pandas as pd
import dask.dataframe as dd
import time
import tracemalloc

filename = "5GB_fake_data.csv"
compressed_filename = "5GB_fake_data.gz"

def run_with_memory(func):
    tracemalloc.start()
    start_time = time.time()

    result = func()

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    elapsed = time.time() - start_time
    peak_gb = peak / (1024 ** 2)

    return elapsed, peak_gb, result

# -------------------------------
# Pandas with chunksize
# -------------------------------
def pandas_chunks():
    total_rows = 0
    for chunk in pd.read_csv(filename, chunksize=1000):
        total_rows += len(chunk)
    return total_rows

pandas_time, pandas_peak, total_rows = run_with_memory(pandas_chunks)
print(f"Pandas (chunks): {pandas_time:.2f}s, Peak RAM: {pandas_peak:.2f} MB")

# -------------------------------
# Dask
# -------------------------------
def dask_read():
    df = dd.read_csv(filename)
    return df.shape[0].compute()

dask_time, dask_peak, total_rows_dask = run_with_memory(dask_read)
print(f"Dask: {dask_time:.2f}s, Peak RAM: {dask_peak:.2f} MB")

# -------------------------------
# Compressed
# -------------------------------
def compressed_read():
    df = pd.read_csv(compressed_filename, compression="gzip")
    return len(df)

comp_time, comp_peak, compressed_rows = run_with_memory(compressed_read)
print(f"Gzip: {comp_time:.2f}s, Peak RAM: {comp_peak:.2f} MB")

# -------------------------------
# Summary
# -------------------------------
print("\n--- Summary ---")
print(f"Pandas (chunks=1k) : {pandas_time:.2f}s | {pandas_peak:.2f} MB")
print(f"Dask               : {dask_time:.2f}s | {dask_peak:.2f} MB")
print(f"Compression        : {comp_time:.2f}s | {comp_peak:.2f} MB")