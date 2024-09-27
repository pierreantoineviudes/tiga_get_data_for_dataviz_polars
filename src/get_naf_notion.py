"""
    MODULE DOCSTRING
"""

import os
import json
import re
from dotenv import load_dotenv
import requests
from tqdm import tqdm


def get_urls_from_notion():
    """_summary_"""
    load_dotenv("./.env")
    with open("./data/urls.json", encoding="UTF-8") as f:
        urls = json.load(f)["urls"]
    notion_token = os.environ["NOTION_TOKEN"]

    codes_naf = []
    codes_sp = []
    definitions = []
    groupes = []
    groupes_distinct = []

    for url in tqdm(urls):

        block_id = url[112:]

        url = f"https://api.notion.com/v1/blocks/{block_id}"
        headers = {
            "Accept": "application/json",
            "Notion-Version": "2022-02-22",
            "Authorization": notion_token,
        }

        response = requests.get(url, headers=headers, timeout=5)
        text = response.text
        # nom_groupe est le nom que l'on voulait donner au groupement de code NAF (un par URL)
        nom_groupe = response.json()["numbered_list_item"]["rich_text"][0]["text"][
            "content"
        ]
        groupes_distinct.append(nom_groupe)

        # codes_list est la liste des codes naf récupérés sur la page
        codes_list = re.findall(r"\d\d\d\d[A-Z]", text)
        codes_list = codes_list[: (len(codes_list) // 2)]
        # definition est la définition donnée au code NAF correspondant
        definition = re.findall(r"\((.*?)\)", text)
        definition = definition[: (len(definition) // 2)]
        definition = definition[1:]

        definitions += definition

        for code in codes_list:
            codes_sp.append(code)
            code_naf = code[0:2] + "." + code[2:]
            codes_naf.append(code_naf)
            groupes.append(nom_groupe)

    with open("output/interesting_naf_codes.csv", "w", encoding="utf-8") as file:
        file.write("Code NAF|Définition|Groupe \n")
        for i in range(len(codes_sp)):
            file.write(codes_sp[i])
            file.write("|")
            file.write(definitions[i])
            file.write("|")
            file.write(groupes[i])
            file.write("\n")
