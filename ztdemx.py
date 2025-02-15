import os
import time 
import dotenv
import pandas as pd
import subprocess

# Directory settings
from upaths import outdir_tandemx



def download_tandemx_dem(file_urls, odir, X=30):
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
    if X == 30:
        username = os.getenv("COPERNICUS_USERNAME")
        password = os.getenv("COPERNICUS_PASSWORD")

    elif X == 90:
        username = os.getenv("COPERNICUS_USERNAME_90")
        password = os.getenv("COPERNICUS_PASSWORD_90")

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

def notify_send(title: str, message: str, duration: int = 5):
    """
    Displays a notification on Linux using notify-send.
    
    Parameters:
    title (str): The notification title.
    message (str): The notification message.
    duration (int): Time in seconds to display the notification.
    """
    os.system(f'notify-send -t {duration * 1000} "{title}" "{message}"')



# add notification or send email

ti = time.perf_counter()

id = 5#2#1#3#4

for id in range(1,26):
    print(id)
    pathdir = f"/home/ljp238/Downloads/tdem_batches/batch_{id}/"
    outdir = f"/media/ljp238/12TBWolf/RSPROX/TANDEMX_EDEM/batch_{id}"
    os.makedirs(outdir, exist_ok=True)
    txtfile = f'{pathdir}/batch_{id}.txt'

    time.sleep(5)


    if os.path.isfile(txtfile):
        print(txtfile)
        ta = time.perf_counter()

        print('file good')
        urls = pd.read_csv(txtfile).squeeze().tolist(); print(len(urls))
        #urls = urls[:2]
        print(len(urls))
        download_tandemx_dem(file_urls=urls, odir=outdir, X=30)
        tb = time.perf_counter() - ta
        print(f'run.time {tb/60} mins')

    time.sleep(15)


tf = time.perf_counter() - ti
print(f'run.time {tf/60} mins')
print('outdir')


# # Example usage
# notify_send("Python Script Finished", 
#             f"Your script has completed execution.\n{outdir}", 5000)
