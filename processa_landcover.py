import os
import traceback
from time import time
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from auto_landcover_tools import preenche_atributos_raster, preenche_atributos_vetorial, busca_estradas

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

def processa_landcover(lista_fontes, roads_in=None):
    # Iniciando processamento
    print("\nIniciando processamento...\n")
    inicio = time()
    
    # Leitura do banco de dados
    print("Realizando leitura do banco de dados...")
    conn_anotacoes_sr_db = create_engine(f'postgresql://{USER}:{ANOTACOES_SR_DB_PASSWORD}@{ANOTACOES_SR_DB_HOST}:{PORT}/{ANOTACOES_SR_DB_NAME}')
    # conn_gisdb = create_engine(f'postgresql://{USER}:{GISDB_GISREP_PASSWORD}@{GISREP_GISDB_HOST}:{PORT}/{GISDB_NAME}')
    # conn_gisrep = create_engine(f'postgresql://{USER}:{GISDB_GISREP_PASSWORD}@{GISREP_GISDB_HOST}:{PORT}/{GISREP_NAME}')

    # # Consultando bases
    # print("Consultando base de grãos...")
    # base_graos = gpd.GeoDataFrame.from_postgis("""
    #                                                SELECT *
    #                                                FROM "bmp11"."tb_grbrasil_112023"
    #                                                """,
    #                                                con=conn_gisdb, geom_col="geom", crs=4674).rename(columns={"geom":"geometry"}).set_geometry("geometry").to_crs("EPSG:4326")

    # print("Consultando base canasat...\n")
    # base_cnsat = gpd.GeoDataFrame.from_postgis("""
    #                                            SELECT *
    #                                            FROM "cst14"."tb_cnsat_142023"
    #                                            """,
    #                                            con=conn_gisdb, geom_col="geom", crs=4674).rename(columns={"geom":"geometry"}).set_geometry("geometry").to_crs("EPSG:4326")
    # base_cnsat["cultura"] = "cana" # Adicionando coluna "cultura para padronizar com base de grãos"

    # # Tempo de carregamento
    # carregamento = time()
    # print(f"Tempo de carregamento dos dados vetoriais: {carregamento-inicio} segundos\n")

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
    # Adicionando nova coluna
    gdf_final["classes_possiveis"] = None
    
    total_erros = 0
    for fonte in lista_fontes:
        # Análise de uso e cobertura
        print("Iniciando uso e cobertura...")
        parcial = time()
        contador = 1
        erros = 0
        for interest_area_id in lista_id:
            print(f"Preenchendo área {contador} de {len(lista_id)} ({fonte}) - ID {interest_area_id}.")
            try:
                area_de_interesse = areas_de_interesse[areas_de_interesse["interest_area_id"] == interest_area_id].reset_index(drop=True)

                ############ zerando as colunas da área de interesse
                area_de_interesse[["class", "conversion_year", "irrigation",
                                "crops_per_year", "created_by", "created_at",
                                "modified_by", "modified_at", "paved_road"]] = None
                # Adicionando nova coluna
                area_de_interesse["classes_possiveis"] = None

                # Filtragem espacial das bases
                # dissolve_area_de_interesse = area_de_interesse.dissolve().reset_index(drop=True).loc[0, "geometry"]
                # base_graos_filtrada = base_graos[base_graos.intersects(dissolve_area_de_interesse)]
                # base_cnsat_filtrada = base_cnsat[base_cnsat.intersects(dissolve_area_de_interesse)]

                # Executando funções para preenchimento de campos
                # gdf_out = preenche_atributos_vetorial(area_de_interesse, [base_graos_filtrada, base_cnsat_filtrada])
                # gdf_out = preenche_atributos_raster(gdf_out, r"assets\brasil_sentinel_coverage_2022.tif")
                
                # Preenchendo os campos com raster
                if fonte == "mapbiomas":
                    gdf_out = preenche_atributos_raster(area_de_interesse, lulc_raster_in_path=r"assets\landcover\brasil_sentinel_coverage_2022_mapbiomas.tif", irrigation_raster_in_path=r"assets\irrigacao\processado\irrigacao_ana_mapbiomas.tif", lulc_origem_dict="mapbiomas")
                elif fonte == "simfaz":
                    gdf_out = preenche_atributos_raster(area_de_interesse, lulc_raster_in_path=r"assets\landcover\uso_terra_simfaz_2021.tif", irrigation_raster_in_path=r"assets\irrigacao\processado\irrigacao_ana_mapbiomas.tif", lulc_origem_dict="simfaz")
                elif fonte == "agrosatelite":
                    gdf_out = preenche_atributos_raster(area_de_interesse, lulc_raster_in_path=r"assets\landcover\remote_sensing_landuse_2022_2023_2023_09_30.tif", irrigation_raster_in_path=r"assets\irrigacao\processado\irrigacao_ana_mapbiomas.tif", lulc_origem_dict="agrosatelite")
                
                # Preenchendo paved_road
                if roads_in is None:
                    gdf_out = busca_estradas(gdf_out)
                else:
                    gdf_out = busca_estradas(gdf_out, roads_in=roads_in)

                # Retorna resultados ao SRC de entrada e junta ao gdf final
                gdf_out = gdf_out.to_crs(gdf_final.crs)
                gdf_final = pd.concat([gdf_final, gdf_out], ignore_index=True).set_geometry("geometry").set_crs("EPSG:4326")
                
                # Fim da análise atual
                print(f"Área {contador} de {len(lista_id)} concluída ({fonte}).\n")
            
            except Exception as e:
                erros += 1
                total_erros += 1
                print(e)
                print(f"ERRO - Área {contador} de {len(lista_id)}.\nFONTE - {fonte}\n")
                traceback.print_exc()

            contador += 1
        
        # Finalizando processamento parcial
        print(f"Finalizando processamento {fonte}")
        print(f"Erros nesta sessão: {erros}")
        print(f"Tempo decorrido nesta sessão: {int((time()-parcial)/60)} minutos\n")

        # Arquivo geojson de saída
        gdf_final.to_file(f"saidas\saida_script_{fonte}.geojson")

    # Final processamento
    final = time()
    print("Finalizando processatmento...")
    print(f"Total de erros: {total_erros}")
    print(f"Tempo total: {int((final-inicio)/60)} minutos")