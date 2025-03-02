import boto3
import os
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd
import geopandas as gp
import numpy as np
import matplotlib.pyplot as plt

import rasterio
from rasterio.merge import merge
import rasterio.mask
from PIL import Image
from pyproj import Transformer
from shapely.geometry import box
from shapely.geometry import box, GeometryCollection, MultiPolygon, polygon, shape
import json

tilenames = [
    "N09E105", "N09E106", "N10E104", "N10E105", "N10E106",
    "N11E104", "N11E105", "N12E103", "N12E104", "N12E105",
    "N13E103", "N13E104", "N13E105", "S01W063", "S01W064",
    "S02W063", "S02W064"
]

# a few raster functions

def merge_rasters(files, outfile: str = "test.tif") -> None:
    """
    Merge a list of geotiffs into one file
    """
    src_files_to_mosaic = []
    for fp in files:
        src = rasterio.open(fp)
        src_files_to_mosaic.append(src)

    crs = src.crs
    out_meta = src.meta.copy()
    mosaic, out_trans = merge(src_files_to_mosaic)

    # Update the metadata
    out_meta.update(
        {
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans,
            "crs": crs,
        }
    )

    with rasterio.open(outfile, "w", **out_meta, compress="DEFLATE", BIGTIFF='YES') as dest:
        dest.write(mosaic)

def crop_raster(rasterfile: str, aoifile:str, outfile: str = "test.tif", nodata=255):
    gdf_aoi=gp.read_file(aoifile)
    with rasterio.open(rasterfile) as src:
            out_meta = src.meta.copy()
            if not src.crs == gdf_aoi.crs:
                gdf_aoi=gdf_aoi.to_crs(src.crs)
            aoi=gdf_aoi.iloc[0].geometry
            im, trans = rasterio.mask.mask(
                src, [aoi], crop=True, nodata=nodata, all_touched=True
            )
            # Update the metadata
            out_meta.update(
            {
                "driver": "GTiff",
                "height": im.shape[1],
                "width": im.shape[2],
                "transform": trans,
                "crs": src.crs,
                "nodata": nodata,
            }
            )
    with rasterio.open(outfile, "w", **out_meta, compress="DEFLATE", BIGTIFF='YES') as dest:
        dest.write(im)

def enforce_mask(file, outfile=None, nodata=255):
    if not outfile:
        outfile=file.replace('.tif', 'masked.tif')
    with rasterio.open(file) as src:
        mask=src.read_masks()
        data=src.read()
        #nodata has been assigned as 255 in the S3 .msk files
        data[mask==255]=nodata
        out_meta=src.meta
        out_meta.update(
            {"nodata":nodata}
        )
        with rasterio.open(outfile, "w", **out_meta, compress="DEFLATE") as dest:
            dest.write(data)

import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject, Resampling


def run_with_notification(timeout=5000):
    """
    Run a shell command and send a desktop notification with a timeout when it's done.
    
    :param command: The shell command to run.
    :param timeout: The time in milliseconds for the notification to stay on screen.
    """
    # Run the command
    #os.system(command)

    # Send a notification using os.system() and 'notify-send' on Linux with timeout
    os.system(f'notify-send "Task Completed" "Your task has finished running!" -t {timeout}')


def enforce_resolution(file, outfile=None, res=12):
    if not outfile:
        outfile = file.replace('.tif', f'_res{res}.tif')
    
    with rasterio.open(file) as src:
        src_res = src.res[0]  # Assuming square pixels, so res[0] is sufficient
        
        # Only resample if the current resolution is greater than the desired one
        if src_res < res:
            # Get the scale factor to change the resolution
            scale_factor = src_res / res

            # Create the target transform
            target_transform = src.transform * src.transform.scale(
                (src_res / res), (src_res / res)
            )

            # Prepare the new shape based on target resolution
            target_height = int(src.height * scale_factor)
            target_width = int(src.width * scale_factor)
            
            # Prepare the output array
            out_array = np.empty((src.count, target_height, target_width), dtype=np.float32)

            # Reproject to the new resolution using bilinear resampling
            reproject(
                source=rasterio.band(src, 1), 
                destination=out_array, 
                src_transform=src.transform, 
                src_crs=src.crs, 
                dst_transform=target_transform, 
                dst_crs=src.crs, 
                resampling=Resampling.bilinear
            )

            # Update metadata to reflect the new resolution
            out_meta = src.meta.copy()
            out_meta.update({
                'driver': 'GTiff',
                'height': target_height,
                'width': target_width,
                'transform': target_transform,
                'nodata': src.nodata,
            })
            
            # Save the resampled raster to the output file
            with rasterio.open(outfile, 'w', **out_meta, compress='DEFLATE', BIGTIFF='YES') as dest:
                dest.write(out_array)
