from logging import INFO, basicConfig, getLogger
from os import cpu_count

basicConfig(
    level=INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = getLogger(__name__)

# Base parameter
MAX_WORKERS = int(cpu_count() or 1)
