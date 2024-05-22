import os
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from auto_landcover_tools import busca_estradas

estradas_agrosatelite = False

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
    
    if estradas_agrosatelite:
        area_de_interesse_wkt = area_de_interesse.to_crs("EPSG:4674").unary_union.wkt

        estradas = gpd.GeoDataFrame.from_postgis(f"""
                                        SELECT "id", "geom"
                                        FROM "lgt10"."tb_rodov_1010"
                                        WHERE ST_Intersects("geom", ST_GeomFromText('{area_de_interesse_wkt}', 4674))
                                        """,
                                        con=conn_gisdb,
                                        geom_col="geom",
                                        crs=4674).rename(columns={"geom":"geometry"}).set_geometry("geometry").to_crs("EPSG:4326")

        gdf_out = busca_estradas(area_de_interesse, roads_in=estradas, is_wgs=True)

    else:
        gdf_out = busca_estradas(area_de_interesse, is_wgs=True)

    # Retorna resultados ao SRC de entrada e junta ao gdf final
    gdf_out = gdf_out.to_crs(gdf_final.crs)
    gdf_final = pd.concat([gdf_final, gdf_out], ignore_index=True).set_geometry("geometry").set_crs("EPSG:4326")

# Compara estradas
estradas_script = gdf_final[["id", "paved_road", "geometry"]].rename(columns={"paved_road":"paved_road_s"})
estradas_analistas = gpd.read_file(r"assets\analise_dados\fields_valuation_analistas.geojson")[["id", "paved_road"]].replace({None:"NULL"})

estradas_join = estradas_script.merge(estradas_analistas, on="id")
estradas_join_validas = estradas_join[estradas_join["paved_road"] != 'NULL']

# Exporta join
estradas_join.to_file("paved_road_script_21_2.geojson")

# print(estradas_join_validas)
# print(estradas_join_validas[estradas_join_validas["paved_road"] == estradas_join_validas["paved_road_s"]])

taxa_acerto = (len(estradas_join_validas[estradas_join_validas["paved_road"] == estradas_join_validas["paved_road_s"]])/len(estradas_join_validas)) * 100

print(f"\nTaxa de acerto para paved_road (osm): {taxa_acerto}%")

no_yes = len(estradas_join_validas[(estradas_join_validas["paved_road"] == "NO") & ((estradas_join_validas["paved_road_s"] == "10KM_ROAD") | (estradas_join_validas["paved_road_s"] == "TOUCH_ROAD"))])
touch_no = len(estradas_join_validas[(estradas_join_validas["paved_road"] == "TOUCH_ROAD") & (estradas_join_validas["paved_road_s"] == "NO")])
touch_tenkm = len(estradas_join_validas[(estradas_join_validas["paved_road"] == "TOUCH_ROAD") & (estradas_join_validas["paved_road_s"] == "10KM_ROAD")])
tenkm_no = len(estradas_join_validas[(estradas_join_validas["paved_road"] == "10KM_ROAD") & (estradas_join_validas["paved_road_s"] == "NO")])
tenkm_touch = len(estradas_join_validas[(estradas_join_validas["paved_road"] == "10KM_ROAD") & (estradas_join_validas["paved_road_s"] == "TOUCH_ROAD")])

print(f"Erros onde o analista classificou NO e o script qualquer outro: {no_yes}")
print(f"Erros onde o analista classificou TOUCH_ROAD e o script NO: {touch_no}")
print(f"Erros onde o analista classificou TOUCH_ROAD e o script 10KM_ROAD: {touch_tenkm}")
print(f"Erros onde o analista classificou 10KM_ROAD e o script NO: {tenkm_no}")
print(f"Erros onde o analista classificou 10KM_ROAD e o script TOUCH_ROAD: {tenkm_touch}")