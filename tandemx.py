import os
import dotenv
import pandas as pd
import subprocess

def download_copernicus_dem(file_urls, odir):
    """
    Downloads Copernicus DEM tiles using wget with authentication from .env credentials.
    Skips files that already exist.
    
    Args:
        file_urls (list): List of DEM tile URLs.
        odir (str): Output directory for downloaded files.
    """
    # Load environment variables from .env file
    dotenv.load_dotenv()

    # Get credentials
    username = os.getenv("COPERNICUS_USERNAME")
    password = os.getenv("COPERNICUS_PASSWORD")

    if not username or not password:
        raise ValueError("Missing COPERNICUS_USERNAME or COPERNICUS_PASSWORD in .env file.")

    # Ensure output directory exists
    os.makedirs(odir, exist_ok=True)

    # Filter out files that already exist
    filtered_urls = []
    for url in file_urls:
        filename = os.path.join(odir, os.path.basename(url))
        if not os.path.exists(filename):
            filtered_urls.append(url)

    if not filtered_urls:
        print("All files already exist. Nothing to download.")
        return

    # Write filtered URLs to list.txt
    list_file = os.path.join(odir, "list.txt")
    with open(list_file, "w") as f:
        f.write("\n".join(filtered_urls))

    # Construct wget command with verbosity (-v) and skipping existing files (-nc)
    wget_cmd = [
        "wget", "-i", list_file,
        "--auth-no-challenge",
        "--user", username,
        "--password", password,
        "-P", odir,
        "-v",  # Verbose output
        "-nc"  # Skip existing files
    ]

    # Execute the command
    subprocess.run(wget_cmd, check=True)

# Directory settings
from upaths import outdir_tandemx

outdir = outdir_tandemx

# Define tiles to download
tnames = ['S01W063', 'N13E103','N10E105','N09E106','S02W063']
X = 30  # DEM resolution (30m or 90m) 90 is not working, probably need a thingy to do ti

if X == 30:
    odir = f'{outdir}/TDEMX{X}'
    urls = pd.read_csv('TDM30_EDEM-url-list.txt').squeeze().tolist()
elif X == 90:
    odir = f'{outdir}/TDEMX{X}'
    urls = pd.read_csv('TDM90-url-list.txt').squeeze().tolist()
    print(len(urls))
else:
    raise ValueError("Invalid DEM resolution. Choose 30 or 90.")

# Filter URLs based on requested tile names
furls = sorted(set(i for i in urls for j in tnames if j in i))
assert len(tnames) == len(furls), 'Check files against tile names'

# Download filtered URLs
download_copernicus_dem(furls, odir)
