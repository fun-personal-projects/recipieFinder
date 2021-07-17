import pandas as pd
from pathlib import Path
from argparse import ArgumentParser
from backbone import generate_merged_csv, read_data
import warnings
warnings.filterwarnings("ignore")

ar = ArgumentParser()
ar.add_argument("-p", help="Path to dataset", default="/media/hdd/Datasets/culinarydb")
ar.add_argument("-l", help="Only list all ingredients present", action="store_true")
ar.add_argument("-i", help="Comma separated list of items to add", required=True)
ar.add_argument("-e", help="Comma separated list of items to exclude")
ap = ar.parse_args()

print("Reading data")
main_path = Path(ap.p)/"flattened_recipies.csv"
print("Loaded data")
# Check if database exists, if not then create
if not Path.exists(main_path): generate_merged_csv(ap.p)
# Read it
df = pd.read_csv(main_path)
if ap.l: print(list(df.columns)[3:-1])
else:
    read_data(df, ap.i, ap.e)

