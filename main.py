# Importar bibliotecas
import geopandas as gpd
from processa_landcover import processa_landcover
from auto_landcover_tools import analisa_resultados

# Abrindo estradas
# estradas_dnit = gpd.read_file(r"assets\estradas\dnit_merge.geojson").to_crs("EPSG:4326")
# estradas_agrosatelite = gpd.read_file(r"assets\estradas\rodovias_agrosatelite.geojson").to_crs("EPSG:4326")

# Processar valorações
processa_landcover(["mapbiomas", "simfaz", "agrosatelite"])
print("Arquivos salvos em 'C:\projetos_python\automatiza_landcover\saidas'")

# Carregar arquivos
analistas = gpd.read_file(r"assets\analise_dados\fields_valuation_analistas.geojson")
mapbiomas = gpd.read_file(r"saidas\saida_script_mapbiomas.geojson")
simfaz = gpd.read_file(r"saidas\saida_script_simfaz.geojson")
agrosatelite = gpd.read_file(r"saidas\saida_script_agrosatelite.geojson")

# Resultados
analisa_resultados(analistas=analistas, mapbiomas=mapbiomas, simfaz=simfaz, agrosatelite=agrosatelite)

