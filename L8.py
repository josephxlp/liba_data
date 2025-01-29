import os
import sys
import ee
import geemap
import time
import geopandas as gpd
from glob import glob
from concurrent.futures import ThreadPoolExecutor
from os.path import basename, join, isfile
from upaths import patches_pattern, landsat8_dpath

def initialize_gee_highvolume_api():
    try:
        ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')
    except:
        ee.Authenticate()
        ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')

def get_ee_geometry(i, g, name):
    ig = g.iloc[i:i+1]
    bBox = ig.geometry.bounds.iloc[0].tolist()  # [minx, miny, maxx, maxy]
    fname = basename(ig.location.values[0]).replace('.tif', f'_{name}.tif')
    region = ee.Geometry.Rectangle(bBox)
    return region, fname

def mask_clouds_landsat(image):
    qa = image.select('QA_PIXEL')
    cloudShadowBitMask = 1 << 3
    cloudBitMask = 1 << 5
    mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(qa.bitwiseAnd(cloudBitMask).eq(0))
    return image.updateMask(mask)

def get_Landsat8_median(region, CLOUD_FILTER=30,datei='2021-01-01', datef='2021-12-30'):
    ls8coll = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2") \
                .filterBounds(region) \
                .filterDate(datei, datef) \
                .filter(ee.Filter.lt("CLOUD_COVER", CLOUD_FILTER))
    
    count = ls8coll.size().getInfo()  # Check number of images
    if count == 0:
        print("WARNING: No Landsat 8 images found for this region!")
        sys.exit()
        return None  # Return None if empty

    ls8_masked = ls8coll.map(mask_clouds_landsat)
    
    # Select bands explicitly
    bands = ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10']
    rgb = ls8_masked.select(bands).median().clip(region)

    return rgb

def gee_download_geemap(image, outpath, scale):
    if isfile(outpath):
        print(f'Skipping (already exists): {outpath}')
        return
    
    try:
        geemap.ee_export_image(image, outpath, scale=scale)
        print(f'Downloaded: {outpath}')
    except Exception as e:
        print(f"Download failed for {outpath}: {e}")

def download_landsat8(i, g, name, tile_path, scale):
    region, fname = get_ee_geometry(i, g, name)
    rgb = get_Landsat8_median(region)
    
    if rgb is None:  # Skip if no valid images
        print(f"Skipping {fname} due to no data.")
        return
    
    outpath = join(tile_path, fname)
    gee_download_geemap(rgb, outpath, scale)

# -------------------------------------------------------------#
initialize_gee_highvolume_api()

cpus = int(os.cpu_count() * 0.75)
scale = 30
name = 'L8_AllBands'

landsat8_dpath = landsat8_dpath  # Update with actual path
gpkg_files = sorted(glob(patches_pattern), reverse=True)  # Update with actual pattern

if __name__ == '__main__':
    os.makedirs(landsat8_dpath, exist_ok=True)
    ti = time.perf_counter()
    for j, gfile in enumerate(gpkg_files):
        #if j > 0: break  # Process one file for testing

        g = gpd.read_file(gfile)
        g[['minx', 'miny', 'maxx', 'maxy']] = g.bounds
        print(f'Processing: {gfile}')
        
        tname = os.path.basename(gfile).replace('.gpkg', '')
        tile_path = os.path.join(landsat8_dpath, tname)
        os.makedirs(tile_path, exist_ok=True)

        with ThreadPoolExecutor(cpus) as TEX:
            for i in range(g.shape[0]):
                TEX.submit(download_landsat8, i, g, name, tile_path, scale)
                #if i > 100: break  # Process first 100 for testing

    tf = time.perf_counter() - ti
    print(f'Execution Time: {tf/60:.2f} mins')
