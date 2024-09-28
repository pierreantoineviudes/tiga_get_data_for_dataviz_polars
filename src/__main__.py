"""
    This module executes the scripts to process the data using polars 
"""

from get_naf_notion import get_urls_from_notion
from process_data import process_geoloc, process_etablissements, merge_dataframes

if __name__ == "__main__":
    departements = [59, 62]
    get_urls_from_notion()
    process_geoloc(departements=departements)
    process_etablissements(departements=departements)
    merge_dataframes()
