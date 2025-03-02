import os
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import time
import pandas as pd
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import enforce_mask, enforce_resolution, run_with_notification
from uvars import outdir, patches_fn, block_name

ta = time.perf_counter()
print('=============================================================================')
print(f'running {block_name}')

os.makedirs(outdir, exist_ok=True)

s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket = 'dataforgood-fb-data'
localdir = outdir  # 'data'
os.makedirs(localdir, exist_ok=True)
gfile_gn = os.path.join(outdir, "tiles.geojson")

s3file = 'forests/v1/alsgedi_global_v6_float/tiles.geojson'
localfile = f"{localdir}/{os.path.basename(s3file)}"
if not os.path.exists(localfile):
    s3_client.download_file(bucket, s3file, localfile)

tiles = gpd.read_file(gfile_gn)
target = gpd.read_file(patches_fn)

if tiles.crs != target.crs:
    target = target.to_crs(tiles.crs)

target_tiles = gpd.sjoin(tiles, target.loc[:, :])  # 0
target_tiles.nunique()

# Download data from AWS S3
s3chmpath = 'forests/v1/alsgedi_global_v6_float/chm'
s3mskpath = 'forests/v1/alsgedi_global_v6_float/msk'
s3metapath = 'forests/v1/alsgedi_global_v6_float/metadata'
tifs = []
tres = []
metas = []
res = 12
times = []

# Get the unique tiles from the target_tiles
unique_tiles = target_tiles['tile'].unique()

# Function to process each tile
def process_tile(tile):
    ti = time.perf_counter()
    print(f"Processing tile: {tile}")
    
    # Download CHM
    s3file = f"{s3chmpath}/{tile}.tif"
    localfile = f"{localdir}/{os.path.basename(s3file)}"
    if not os.path.exists(localfile):
        s3_client.download_file(bucket, s3file, localfile)

    # Download cloud masks
    mskfile = f"{s3mskpath}/{tile}.tif.msk"
    localmskfile = f"{localdir}/{os.path.basename(mskfile)}"
    if not os.path.exists(localmskfile):
        s3_client.download_file(bucket, mskfile, localmskfile)

    # Download metadata
    jsonfile = f"{s3metapath}/{tile}.geojson"
    localjsonfile = f"{localdir}/{os.path.basename(jsonfile)}"
    if not os.path.exists(localjsonfile):
        s3_client.download_file(bucket, jsonfile, localjsonfile)

    # Apply mask to the downloaded CHM
    outfile = localfile.replace('.tif', 'masked.tif')
    if not os.path.exists(outfile):
        enforce_mask(localfile, outfile)

    # Apply resolution adjustment
    res_outfile = localfile.replace('.tif', f'masked_{str(res)}.tif')
    if not os.path.exists(res_outfile):
        enforce_resolution(localfile, outfile=res_outfile, res=res)
    
    tf = time.perf_counter() - ti
    print(f'Tile Download Time {tf/60} min(s)')
    return localfile, localjsonfile, res_outfile, tf

# Create a ThreadPoolExecutor to process tiles in parallel
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_tile, tile) for tile in unique_tiles]
    
    # Collect results as they finish
    for future in as_completed(futures):
        localfile, localjsonfile, res_outfile, tf = future.result()
        metas.append(localjsonfile)
        tifs.append(localfile.replace('.tif', 'masked.tif'))
        tres.append(res_outfile)
        times.append(tf)

# Write tifs and metas to CSV file
df = pd.DataFrame({'tif': tifs, 'meta': metas, 'restif': tres, 'times': times})
df.to_csv(f"{localdir}/{block_name}.csv", index=False)

tb = time.perf_counter() - ta
print(f'RUN.TIME {tb/60} min(s)')
run_with_notification(timeout=5000)
