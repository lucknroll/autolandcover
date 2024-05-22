import os
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from auto_landcover_tools import preenche_atributos_raster

raster_irrigacao = r"assets\irrigacao\processado\irrigacao_ana_mapbiomas_22_final.tif"

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
    
    gdf_out = preenche_atributos_raster(area_de_interesse, irrigation_raster_in_path=raster_irrigacao)

    # Retorna resultados ao SRC de entrada e junta ao gdf final
    gdf_out = gdf_out.to_crs(gdf_final.crs)
    gdf_final = pd.concat([gdf_final, gdf_out], ignore_index=True).set_geometry("geometry").set_crs("EPSG:4326")

# Compara irrigacao
irrigacao_script = gdf_final[["id", "irrigation", "classes_possiveis", "geometry"]].rename(columns={"irrigation":"irrigation_s"})
irrigacao_analistas = gpd.read_file(r"assets\analise_dados\fields_valuation_analistas.geojson")[["id", "irrigation"]].replace({None:"NULL"})

irrigacao_join = irrigacao_script.merge(irrigacao_analistas, on="id")
irrigacao_join_validas = irrigacao_join[irrigacao_join["irrigation"] != 'NULL']

irrigacao_join[["irrigation", "irrigation_s", "classes_possiveis", "geometry"]].to_file("irrigation_script_21.geojson")

print(irrigacao_join_validas)
print(irrigacao_join_validas[irrigacao_join_validas["irrigation"] == irrigacao_join_validas["irrigation_s"]])

taxa_acerto = (len(irrigacao_join_validas[irrigacao_join_validas["irrigation"] == irrigacao_join_validas["irrigation_s"]])/len(irrigacao_join_validas)) * 100

# Printa acertos e erros
print(f"\nTaxa de acerto para irrigation (mapbiomas+ana): {taxa_acerto}%")

yes_no = len(irrigacao_join_validas[(irrigacao_join_validas["irrigation"] == "YES") & (irrigacao_join_validas["irrigation_s"] == "NO")])
no_yes = len(irrigacao_join_validas[(irrigacao_join_validas["irrigation"] == "NO") & (irrigacao_join_validas["irrigation_s"] == "YES")])
print(f"Erros onde o analista classificou NO e o script YES: {no_yes}")
print(f"Erros onde o analista classificou YES e o script NO: {yes_no}")