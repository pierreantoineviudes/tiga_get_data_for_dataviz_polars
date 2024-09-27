"""
    This module contains the necessary functions to execute data processing on the csv
"""

import os
from typing import List
import csv
import polars as pl


# ---------------- DEFINE PATHS -------------------------------------------------------------------
GEOLOC_PATH = r".\data\GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8\GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.csv"
PATH_ETABLISSEMENTS = r".\data\StockEtablissement_utf8\StockEtablissement_utf8.csv"
DIR_ETABLISSEMENTS = r".\data\output\etablissement"
DIR_GEOLOC = r".\data\output\geoloc"
OUTPUT_PATH = r".\data\output\ouput.csv"
# -------------------------------------------------------------------------------------------------


def process_geoloc(departements: List[int]) -> None:
    """processed the geolocated dataframe

    Args:
        departements (List[int]): list of departements to keep
    """
    reader = pl.read_csv_batched(
        GEOLOC_PATH,
        separator=";",
        ignore_errors=True,
        columns=["siret", "x_longitude", "y_latitude", "plg_code_commune"],
    )
    batches = reader.next_batches(100)
    # trouver le nb de batches qui rend le truc le + rapide
    i = 0
    while batches:
        df_current_batches = pl.concat(batches)
        # process df
        df_current_batches = (
            df_current_batches.lazy()
            .with_columns(
                (df_current_batches["plg_code_commune"] // 1000).rename("departement")
            )
            .filter(pl.col("departement").is_in(departements))
            .collect()
        )

        # save df
        df_current_batches.write_csv(
            file=f"./data/output/geoloc/geoloc_out_{i}.csv", include_header=True
        )
        i += 1
        batches = reader.next_batches(100)


def process_etablissements(departements: List[int]) -> None:
    """processed the etablissement dataframe

    Args:
        departements (List[int]): list of departements to keep
    """
    headers_to_keep = [
        "siren",
        "siret",
        "trancheEffectifsEtablissement",
        "activitePrincipaleEtablissement",
        "codeCommuneEtablissement",
    ]
    naf_codes = []
    with open("output/interesting_naf_codes.csv", "r", encoding="utf-8") as file:
        csv_reader = csv.DictReader(file, delimiter="|")
        for line in csv_reader:
            naf_codes.append(line["Code NAF"][:2] + "." + line["Code NAF"][2:])
    reader = pl.read_csv_batched(
        PATH_ETABLISSEMENTS, separator=",", ignore_errors=True, columns=headers_to_keep
    )
    batches = reader.next_batches(100)
    i = 0
    while batches:
        df_current_batches = pl.concat(batches)

        # process df
        df_current_batches = (
            df_current_batches.lazy()
            .with_columns(
                (df_current_batches["codeCommuneEtablissement"] // 1000).rename(
                    "departement"
                )
            )
            .filter(pl.col("departement").is_in(departements))
            .filter(pl.col("trancheEffectifsEtablissement").is_not_null())
            .filter(pl.col("activitePrincipaleEtablissement").is_in(naf_codes))
            .collect()
        )
        # save df
        df_current_batches.write_csv(
            file=f"./data/output/etablissement/etablissement_{i}.csv",
            include_header=True,
        )
        i += 1
        batches = reader.next_batches(100)


def merge_dataframes() -> None:
    """function to merge dataframes"""
    dict_rh = {
        0: 0,
        1: 2,
        2: 4,
        3: 8,
        11: 15,
        12: 35,
        21: 75,
        22: 150,
        31: 225,
        32: 350,
        41: 750,
        42: 1500,
        51: 3500,
        52: 7500,
        53: 10000,
    }
    paths_etablissement = os.listdir(DIR_ETABLISSEMENTS)
    list_df = []
    for path_file_etablissement in paths_etablissement:
        path_etablissement = os.path.join(DIR_ETABLISSEMENTS, path_file_etablissement)
        list_df.append(pl.read_csv(path_etablissement))
    df_etablissements = pl.concat(list_df)

    paths_geoloc = os.listdir(DIR_GEOLOC)
    list_df = []
    for path_file_geoloc in paths_geoloc:
        path_geoloc = os.path.join(DIR_GEOLOC, path_file_geoloc)
        list_df.append(pl.read_csv(path_geoloc))
    df_geoloc = pl.concat(list_df).lazy()
    df_total = (
        df_etablissements.lazy()
        .join(df_geoloc, on=["siret", "departement"])
        .rename(
            {
                "x_longitude": "lng",
                "y_latitude": "lat",
                "trancheEffectifsEtablissement": "RH",
                "activitePrincipaleEtablissement": "NAF",
            }
        )
        .with_columns(pl.col("RH").replace_strict(dict_rh))
        .collect()
    )
    df_total.write_csv(OUTPUT_PATH)
