# layer location prefix
from concurrent.futures import ThreadPoolExecutor
from math import inf
from subprocess import check_call

from job.utils import MAX_WORKERS, logger

PREFIX = "/usr/src/app/data"

# Output prefix
OUTPUT = "/usr/src/app/output"

# layers conversion parameter
LAYERS = [
    dict(
        name="DEMNAS_Jawa_Timur",
        suffix="Copy of [LapakGIS.com]_DEMNAS_Jawa_Timur_/[LapakGIS.com]_DEMNAS_Jawa_Timur_.tif",
        type="image",
        hierarchy=False,
        multi_file=False,
    ),
    dict(
        name="bathymetry_indonesia",
        suffix="Data Peta Batimetri Seluruh Indonesia/BATIMETRI_NASIONAL_MSL_MOSAIC.tif",
        type="image",
        hierarchy=False,
        multi_file=False,
    ),
    dict(
        name="plantation_indonesia",
        suffix="SHP Peta Perkebunan Pohon Indonesia [lapakgis.com]/Perkebunan Pohon Indonesia.shp",
        type="shapefile",
        hierarchy=False,
        multi_file=False,
    ),
    dict(
        name="vegetation_landuse",
        suffix="Vegetasi (Penggunaan Lahan Eksisting) Indonesia - Lapak GIS",
        type="shapefile",
        hierarchy=True,
        multi_file=True,
    ),
    dict(
        name="mangrove",
        suffix="[LapakGIS.com] Kawasan Hutan Mangrove",
        type="shapefile",
        hierarchy=False,
        multi_file=True,
        source_ids=["Hutan_Mangrove_25K", "Hutan_Mangrove_50K", "Hutan_Mangrove_250K"],
        sub_ids=["25k", "50k", "250k"],
    ),
    dict(
        name="built_up_indonesia",
        suffix="[LapakGIS.com] Lingkungan Terbangun Indonesia",
        type="shapefile",
        hierarchy=False,
        multi_file=True,
        source_ids=[
            "Permukiman 25K (164)",
            "Permukiman 50K (166)",
            "Permukiman Penduduk 250K (167)",
        ],
        sub_ids=["25k", "50k", "250k"],
    ),
    dict(
        name="public_facilities_indonesia",
        suffix="[LapakGIS.com] Shapefile Fasilitas Umum",
        type="shapefile",
        hierarchy=True,
        multi_file=True,
    ),
    dict(
        name="road_indonesia",
        suffix="Shapefile Jalan Indonesia [Lapak GIS.com]",
        type="shapefile",
        hierarchy=False,
        multi_file=True,
        source_ids=[
            "Permukiman 25K (164)",
            "Permukiman 50K (164)",
            "Ruas Jalan 250k (83)",
            "Ruas Jalan Utama 250k (82)",
        ],
        sub_ids=["25k", "50k", "250k", "main_250k"],
    ),
    dict(
        name="geological_indonesia",
        suffix="SHP Shapefile Peta Geologi Se-Indonesia [lapakgis.com]",
        type="shapefile",
        hierarchy=False,
        multi_file=True,
        source_ids=[
            "[LapakGIS.com] GEOLOGI_REGIONAL_250K",
            "[LapakGIS.com] JENIS SESAR",
            "[LapakGIS.com] TEKNIKA NEOGEN",
            "[LapakGIS.com] TEKNIKA OROGEN",
        ],
        sub_ids=["regional", "sesar", "neogen", "orogen"],
    ),
]


def raster_cmd(input_path, output_path):
    logger.info(f"Running {output_path}")
    cmd = f"""gdal raster reproject \
                    --overwrite \
                    -d EPSG:4326 \
                    -f COG \
                    --co="BIGTIFF=YES" \
                    --co="COMPRESS=ZSTD" \
                    --co="STATISTICS=YES" \
                    --co="OVERVIEWS=IGNORE_EXISTING" \
                    --co="OVERVIEW_RESAMPLING=LANCZOS" \
                    --co="RESAMPLING=LANCZOS" \
                    "{input_path}" \
                    "{output_path}"
            """
    check_call(cmd, shell=True)


def vector_cmd(input_path, output_path):
    logger.info(f"Running {output_path}")
    cmd = f"""gdal vector pipeline \
                    ! read "{input_path}" \
                    ! make-valid \
                    ! explode-collections \
                    ! set-geom-type --single \
                    ! reproject -d EPSG:4326 \
                    ! write --overwrite -f FlatGeobuf "{output_path}" \
            """
    check_call(cmd, shell=True)


def process_layer(layer_dict: dict):
    layer_name = layer_dict["name"]
    suffix = layer_dict["suffix"]
    type = layer_dict["type"]
    hierarchy = layer_dict["hierarchy"]
    multi_file = layer_dict["multi_file"]
    input_path = f"{PREFIX}/{suffix}"

    if type == "image":
        file_format = ".tif"
        function_cmd = raster_cmd
    elif type == "shapefile":
        file_format = ".fgb"
        function_cmd = vector_cmd

    if (not hierarchy) and (not multi_file):
        output_path = f"{OUTPUT}/{layer_name}{file_format}"
        # function_cmd(input_path, output_path)
    elif (not hierarchy) and (multi_file):
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            jobs = []
            for index in range(len(layer_dict["source_ids"])):
                path = layer_dict["source_ids"][index]
                path = f"{input_path}/{path}.shp"
                logger.info(path)
                output_path = (
                    f"{OUTPUT}/{layer_name}_{layer_dict['sub_ids'][index]}{file_format}"
                )
                # jobs.append(executor.submit(function_cmd, input_path, output_path))
            for job in jobs:
                try:
                    job.result()
                except Exception as e:
                    logger.info(f"Error: {e}")


def main():
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        jobs = []
        for layer in LAYERS:
            jobs.append(executor.submit(process_layer, layer))

        for job in jobs:
            try:
                job.result()
            except Exception as e:
                logger.info(f"Error: {e}")


if __name__ == "__main__":
    main()
