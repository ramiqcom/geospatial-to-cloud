from utils import logger, MAX_WORKERS
from concurrent.futures import ThreadPoolExecutor

# layer location prefix
PREFIX = "/usr/src/app/data"

# layers conversion parameter
LAYERS = [
    dict(
        name="DEMNAS_Jawa_Timur",
        suffix="Copy of [LapakGIS.com]_DEMNAS_Jawa_Timur_/[LapakGIS.com]_DEMNAS_Jawa_Timur_.tif",
        type="tif",
    ),
    dict(
        name="public_facilities",
        prefix="[LapakGIS.com] Shapefile Fasilitas Umum",
        type="shapefile",
    ),
]

def main():
  ""

if __name__ == "__main__":
  main()
