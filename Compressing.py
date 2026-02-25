import pandas as pd

filename = "5GB_fake_data.csv"
compressed_filename = "5GB_fake_data.gz"

print("Compressing CSV...")
df_full = pd.read_csv(filename)
df_full.to_csv(compressed_filename, index=False, compression="gzip")
print("Compressing done.\n")