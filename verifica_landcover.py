import os
import pandas as pd
import geopandas as gpd
from time import time
from sqlalchemy import create_engine
from dotenv import load_dotenv
from auto_landcover_tools import preenche_atributos_raster, preenche_atributos_vetorial

# Timer
inicio = time()

# Fontes de dados
vetor = False
fields = False
frankenstein = False
fonte = "agrosatelite"
raster_mapbiomas = r"assets\landcover\brasil_sentinel_coverage_2022_mapbiomas.tif"
raster_simfaz = r"assets\landcover\uso_terra_simfaz_2021.tif"
raster_agrosatelite = r"assets\landcover\remote_sensing_landuse_2022_2023_2023_09_30.tif"

# Carregando Variáveis de ambiente
load_dotenv(".env")
USER = os.environ.get("USER")
GISDB_GISREP_PASSWORD = os.environ.get("GISDB_GISREP_PASSWORD")
ANOTACOES_SR_DB_PASSWORD = os.environ.get("ANOTACOES_SR_DB_PASSWORD")

GISDB_NAME = os.environ.get("GISDB_NAME")
GISREP_NAME = os.environ.get("GISREP")
GISREP_GISDB_HOST = os.environ.get("GISREP_GISDB_HOST")
ANOTACOES_SR_DB_NAME = os.environ.get("ANOTACOES_SR_DB_NAME")
ANOTACOES_SR_DB_HOST = os.environ.get("ANOTACOES_SR_DB_HOST")
PORT = os.environ.get("PORT")

conn_anotacoes_sr_db = create_engine(f'postgresql://{USER}:{ANOTACOES_SR_DB_PASSWORD}@{ANOTACOES_SR_DB_HOST}:{PORT}/{ANOTACOES_SR_DB_NAME}')
conn_gisdb = create_engine(f'postgresql://{USER}:{GISDB_GISREP_PASSWORD}@{GISREP_GISDB_HOST}:{PORT}/{GISDB_NAME}')
conn_gisrep = create_engine(f'postgresql://{USER}:{GISDB_GISREP_PASSWORD}@{GISREP_GISDB_HOST}:{PORT}/{GISREP_NAME}')

# Obtendo áreas de interesse
areas_de_interesse = gpd.GeoDataFrame.from_postgis("""
                                        SELECT *
                                        FROM "remote_sensing"."fields_valuation"
                                        """,
                                        con=conn_anotacoes_sr_db,
                                        geom_col="geom",
                                        crs=4326).rename(columns={"geom":"geometry"}).set_geometry("geometry")

# Lista de áreas de interesse a serem valoradas
lista_id = list(areas_de_interesse["interest_area_id"].unique())

# Preparando geodataframe que irá receber a saída da análise de uso e cobertura (uma cópia do original)
gdf_final = areas_de_interesse.copy()
gdf_final = gdf_final[0:0]

contador = 0
for interest_area_id in lista_id:
    contador += 1
    print(f"Preenchendo área {contador} de {len(lista_id)} - ID {interest_area_id}.")

    area_de_interesse = areas_de_interesse[areas_de_interesse["interest_area_id"] == interest_area_id].reset_index(drop=True)

    area_de_interesse[["class", "conversion_year", "irrigation",
                    "crops_per_year", "created_by", "created_at",
                    "modified_by", "modified_at", "paved_road"]] = None
    
    # if vetor:
    #     # Preenchimento de campos com base vetorial
    #     # area_de_interesse = preenche_atributos_vetorial(area_de_interesse, [base_graos_filtrada, base_cnsat_filtrada])
    #     # if fields:
    #     #     area_de_interesse = preenche_atributos_vetorial(area_de_interesse, [fields_monitoring, base_graos, base_cnsat])
    #     if frankenstein:
    #         area_de_interesse = preenche_atributos_vetorial(area_de_interesse, [gdf_frankenstein, fields_monitoring, base_graos, base_cnsat])
    #     else:
    #         area_de_interesse = preenche_atributos_vetorial(area_de_interesse, [base_graos, base_cnsat])

    if fonte == "agrosatelite":
        area_de_interesse = preenche_atributos_raster(area_de_interesse, lulc_raster_in_path=raster_agrosatelite, lulc_origem_dict="agrosatelite")
    elif fonte == "simfaz":
        area_de_interesse = preenche_atributos_raster(area_de_interesse, lulc_raster_in_path=raster_simfaz, lulc_origem_dict="simfaz")
    elif fonte == "mapbiomas":
        area_de_interesse = preenche_atributos_raster(area_de_interesse, lulc_raster_in_path=raster_mapbiomas, lulc_origem_dict="mapbiomas")

    # Retorna resultados ao SRC de entrada e junta ao gdf final
    area_de_interesse = area_de_interesse.to_crs(gdf_final.crs)
    gdf_final = pd.concat([gdf_final, area_de_interesse], ignore_index=True).set_geometry("geometry").set_crs("EPSG:4326")

# Compara classes
classe_script = gdf_final[["id", "class", "geometry"]].rename(columns={"class":"class_s"})
classe_analistas = gpd.read_file(r"assets\analise_dados\fields_valuation_analistas.geojson")[["id", "class"]]

classe_join = classe_script.merge(classe_analistas, on="id")
classe_join_validas = classe_join[classe_join["class"] != 'NULL']

# Padronização de dados
dict_classes = {"DIRTY_PASTURE":"PASTURE",
                "CLEAN_PASTURE":"PASTURE",
                "WATER":"OTHER",
                "PUBLIC_INFRASTRUCTURE":"INFRASTRUCTURE",
                "PRIVATE_INFRASTRUCTURE":"INFRASTRUCTURE",
                "REGENERATION":"NATIVE_VEGETATION",
                "pastagem":"PASTURE",
                "formação florestal":"NATIVE_VEGETATION",
                "lavoura temporária":"ANNUAL_CROPS",
                "mosaico de usos":"INFRASTRUCTURE",
                "formação savânica":"NATIVE_VEGETATION",
                "rio, lago e oceano": "OTHER",
                "silvicultura":"SILVICULTURE",
                "outras áreas não vegetadas":"INFRASTRUCTURE",
                "formação campestre":"NATIVE_VEGETATION",
                "campo alagado e área pantanosa":"NATIVE_VEGETATION",
                "lavoura perene":"PERENNIAL_CROPS",
                "outros usos":"OTHER",
                "soja":"ANNUAL_CROPS",
                "natural florestal":"NATIVE_VEGETATION",
                "natural não florestal":"NATIVE_VEGETATION",
                "água":"OTHER",
                "cana":"SEMIPERENNIAL_CROPS",
                "milho":"ANNUAL_CROPS",
                "café":"PERENNIAL_CROPS",
                "algodão":"ANNUAL_CROPS",
                "arroz":"ANNUAL_CROPS",
                "area_agricola":"PERENNIAL_CROPS",
                "outros usos antrópicos":"OTHER",
                "área urbana":"INFRASTRUCTURE",
                "outros (infraestrutura, água)": "OTHER",
                "vegetação natural não florestal":"NATIVE_VEGETATION",
                "floresta nativa":"NATIVE_VEGETATION",
                "outras culturas temporárias":"ANNUAL_CROPS",
                "culturas permanentes":"PERENNIAL_CROPS",
                None:"NULL",
                "COTTON":"ANNUAL_CROPS",
                "AGRICULTURAL_AREA":"ANNUAL_CROPS",
                "CITRUS":"PERENNIAL_CROPS",
                "ANTHROPIC_EXPOSED_SOIL":"INFRASTRUCTURE",
                "WHEAT":"ANNUAL_CROPS",
                "NON_FOREST_VEGETATION":"NATIVE_VEGETATION",
                "NATURAL_EXPOSED_SOIL":"NATIVE_VEGETATION",
                "OTHER_PERENNIAL_CROPS":"PERENNIAL_CROPS",
                "OTHER_SEMIPERENNIAL_CROPS":"SEMIPERENNIAL_CROPS",
                "SUGAR_CANE":"SEMIPERENNIAL_CROPS",
                "UNCERTAIN":"OTHER",
                "CORN":"ANNUAL_CROPS",
                "COFFEE":"PERENNIAL_CROPS",
                "NATIVE_FOREST":"NATIVE_VEGETATION",
                "OTHERS":"OTHER",
                "RICE":"ANNUAL_CROPS"
                }

# Salvando arquivo
if vetor:
    if frankenstein:
        classe_join.replace(dict_classes).to_file(f"script_frankenstein_e_{fonte}.geojson")
    else:
        classe_join.replace(dict_classes).to_file(f"script_vetores_e_{fonte}.geojson")
else:
    classe_join.replace(dict_classes).to_file(f"script_{fonte}.geojson")

classe_join_validas = classe_join_validas.replace(dict_classes)

# print(classe_join_validas)
# print(classe_join_validas[classe_join_validas["class"] == classe_join_validas["class_s"]])
matches = classe_join_validas[classe_join_validas["class"] == classe_join_validas["class_s"]]

taxa_acerto = round((len(matches)/len(classe_join_validas)) * 100, 2)

if vetor:
    if fields:
        print(f"\nTaxa de acerto total para {fonte} + canasat(2) + grãos + fields (desconsiderando NULLs da classificação original): {taxa_acerto}%")
    elif frankenstein:
        print(f"\nTaxa de acerto total para {fonte} + canasat(2) + grãos + frankenstein (desconsiderando NULLs da classificação original): {taxa_acerto}%")
    else:
        print(f"\nTaxa de acerto total para {fonte} + canasat(2) + grãos (desconsiderando NULLs da classificação original): {taxa_acerto}%")
else:
    print(f"\nTaxa de acerto total para {fonte} (desconsiderando NULLs da classificação original): {taxa_acerto}%")

## Matches por classe
for classe in classe_join_validas["class"].unique():
    classe_original = classe_join_validas[classe_join_validas["class"] == classe]
    classe_script = classe_join_validas[(classe_join_validas["class"] == classe) & (classe_join_validas["class_s"] == classe)]
    taxa_acerto_classe = round((len(classe_script)/len(classe_original)) * 100, 2)
    print(f"Taxa de acerto para a classe {classe}: {taxa_acerto_classe}%")

print(f"\nTempo total de processamento: {int((time()-inicio)/60)} minuto(s)")