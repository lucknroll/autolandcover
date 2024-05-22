import warnings
warnings.simplefilter(action='ignore')

import geopandas as gpd
import pandas as pd
import osmnx as ox
import rasterio as rio
import numpy as np
from rasterio.mask import mask
import scipy.stats

# Config específica para a lib osmnx
ox.config(requests_kwargs={"verify":False})

###########################################################################
# Reprojeta os dados em graus para o CRS utm sirgas correspondente
def grau_para_utm(entrada, **kwargs):
    """
    entrada: geodataframe, geoseries ou geometria que será convertida para utm (sirgas2000)
    kwargs:
        epsg_in: int com código epsg do sistema de referência de coordenadas do dado de entrada
        (necessário para geometria)
    """

    # Kwargs
    epsg_informado = False
    epsg_in = None
    for k, v in kwargs.items():
        if k == "epsg_in":
            epsg_informado = True
            epsg_in = f"EPSG:{v}"

    # Verifica se o CRS de entrada foi informado, caso contrário, obtém do próprio dado
    if not epsg_informado and hasattr(entrada, 'crs'):
        epsg_in = entrada.crs

    # Identificando a classe de entrada
    class_in = entrada.__class__.__name__

    # Calcula o centróide da entrada para determinar o fuso UTM
    if class_in in ['GeoDataFrame', 'GeoSeries']:
        centroid_in = entrada.unary_union.centroid
    else:
        entrada = gpd.GeoSeries({0:entrada}).set_crs(epsg_in)
        centroid_in = entrada.unary_union.centroid

    # Carrega a grade UTM e converte para o CRS de entrada
    gdf_grade = gpd.read_file("assets/zonas_utm_br.geojson").to_crs(epsg_in)

    # Obtém o fuso UTM pelo centróide do conjunto de dados
    grade_within = gdf_grade[gdf_grade.contains(centroid_in)].reset_index(drop=True)
    epsg_out = grade_within.loc[0, "EPSG_S2000"]

    # Converte o dado de entrada para o CRS UTM
    saida = entrada.to_crs(epsg_out)

    # Retorna a saída na mesma classe da entrada
    if class_in == 'GeoDataFrame':
        return saida
    elif class_in == 'GeoSeries':
        return saida
    else:  # Se for uma instância de geometria
        return saida.geometry.iloc[0]



################################################################################
# Função buffer para recortar e limitar área de processamento
def gera_buffer(entrada, distancia_m, **kwargs):
    """
    gdf_in: gdf de entrada, com sistema de referência em utm (GeoDataFrame)
    distancia_m: distancia em metros do buffer (int)
    epsg_out (opcional): número epsg para a geometria de saída (int)
    utm_in (opcional): booleano, informa se o dado de entrada está em utm
    is_geometry (opcional) booleano, informa se o dado de entrada é uma geometria
    """

    # Kwargs
    set_epsg_out = False
    utm_in = False
    is_geometry = False
    for k, v in kwargs.items():
        if k == "epsg_out":
            set_epsg_out = True
            epsg_out = v
        if k == "is_utm":
            utm_in = v
        if k == "crs_in":
            entrada_crs = v
        if k == "is_geometry":
            is_geometry = v

    # Passando para utm se a entrada for em graus
    if utm_in:
        entrada_utm = entrada
    
    # Se for geometria, converte a geometria para utm
    elif (is_geometry) and (not utm_in):
        entrada = gpd.GeoSeries({0:entrada}).set_crs("EPSG:4326")
        entrada_utm = grau_para_utm(entrada)
        entrada_crs = entrada_utm.crs
        entrada_utm = entrada_utm.iloc[0]
    
    else:
        entrada_utm = grau_para_utm(entrada, epsg_in=4326)


    # Se geodataframe
    if isinstance(entrada_utm, gpd.GeoDataFrame):
        # Dissolvendo geometrias no entrada_utm
        try:
            entrada_dissolve = entrada_utm.dissolve()
        except:
            # tenta achar a geometria inválida e a apaga do geodataframe
            geometrias_invalidas = []
            for idx, row in entrada_utm.iterrows():
                if not row["geometry"].is_valid:
                    geometrias_invalidas.append(idx)
            if len(geometrias_invalidas) > 0:
                entrada_utm = entrada_utm.drop(geometrias_invalidas)
                entrada_dissolve = entrada_utm.dissolve()
        buffer = entrada_dissolve.buffer(distancia_m)
    
    # Se geoseries
    elif isinstance(entrada_utm, gpd.GeoSeries):
        entrada_utm = grau_para_utm(entrada_utm)
        try:
            entrada_dissolve = entrada_utm.unary_union
        except:
            geometrias_invalidas = []
            for idx, geom in enumerate(entrada_gs):
                if not geom.is_valid:
                    geometrias_invalidas.append(idx)
            if len(geometrias_invalidas) > 0:
                entrada_utm = entrada_utm.drop(geometrias_invalidas)
                entrada_dissolve = entrada_utm.unary_union
        buffer = entrada_dissolve.buffer(distancia_m)
        buffer = gpd.GeoSeries({0:buffer}).set_crs(entrada_crs)
    
    # Se geometria
    else:
        entrada_gs = gpd.GeoSeries({0:entrada_utm}).set_crs(entrada_crs)
        buffer = entrada_gs.buffer(distancia_m)
    
    # Conversão de coordenadas
    if set_epsg_out:
        buffer = buffer.to_crs(f"EPSG:{epsg_out}")

    # Retorna uma geometria shapely
    return buffer.iloc[0]
        



###############################################################################
# Função para preenchimento dos atributos a partir dos arquivos vetoriais
def preenche_atributos_vetorial(gdf_in, lista_gdf_fontes):
    """"
    gdf_in: GeoDataFrame de entrada
    lista_gdf_fontes: Lista com GeoDataFrames das fontes de dados
    """

    print("Executando preenche_atributos_vetorial")

    # Cópia do gdf de entrada
    gdf_out = gdf_in.copy()

    # # Define buffer de filtragem dos dados
    # buffer_filtragem = gera_buffer(gdf_in, 10, epsg_out=4326)

    # Itera sobre as fontes de dados
    for fonte_filtrada in lista_gdf_fontes:
        if len(fonte_filtrada) > 0:
           # Aplica um filtro de classe (se já estiver preenchido, não tenta preencher)
            for idx_talhao, talhao in gdf_in[gdf_in["class"] != None].iterrows():
                # Se a geometria for inválida ele pula esse talhão, se for válida aplica um filtro intersects
                if not talhao["geometry"].is_valid:
                    continue
                else:
                    fonte_filtrada_talhao = fonte_filtrada[fonte_filtrada["geometry"].intersects(talhao["geometry"])]
                # Se ainda tiver alguma feição após a filtragem, segue
                if len(fonte_filtrada_talhao) > 0:
                    for _, dado_fonte in fonte_filtrada_talhao.iterrows():
                        dado_fonte_buffer = gera_buffer(dado_fonte["geometry"], 25, epsg_out=4326, is_geometry=True)
                        # Se o talhão se encontrar "within" o buffer da fonte de dados, recebe a sua classificação
                        if talhao["geometry"].within(dado_fonte_buffer):
                            gdf_out.loc[idx_talhao, "class"] = dado_fonte["cultura"]                        # Preenche atributo do talhão
        else:
            print("Sem classificação")

    # Gdf de saída
    return gdf_out



#################################################################################
# Função para preenchimento dos atributos a partir do raster "MAPBIOMAS"
def preenche_atributos_raster(gdf_in, lulc_raster_in_path=None, irrigation_raster_in_path=None, lulc_origem_dict="mapbiomas"):
    """
    gdf_in: geodataframe que será atualizado com os dados (GeoDataFrame)
    lulc_raster_in_path: caminho para o arquivo raster de land use/ land cover (string)
    irrigation_raster_in_path: caminho para o raster de irrigação do mapbiomas (string)
    lulc_origem_dict: indica qual dicionário de dados será usado (mapbiomas ou agrosatélite)
    """

    def analise_raster(raster_in_path, gdf_out, dict_classes, column_name):
        # Abre raster
        raster = rio.open(raster_in_path)

        # Reprojeta gdf para o SRC do raster
        gdf_copy = gdf_out.copy()
        gdf_copy["geometry"] = gdf_copy["geometry"]  # aplicando buffer mínimo para correção de imperfeições
        gdf_copy = gdf_copy.to_crs(raster.crs)

        # Itera sobre os talhões para clipar o raster e calcular a classe mais frequente
        for idx_talhao, talhao in gdf_copy.iterrows():
            if talhao[column_name] is None:          # Só executa se a classe do talhão já não tiver sido preenchida
                clip_geom = talhao["geometry"]            # Polígono em formato geojson para clip
                
                # Se a geometria for inválida, tratar
                if not clip_geom.is_valid:
                    print("geometria inválida")
                    gdf_out.loc[idx_talhao, column_name] = "GEOM_INVÁLIDA"
                    continue
                    
                raster_out, _ = mask(raster, [clip_geom], crop=True, nodata=255, all_touched=True)             # Realiza o clip do raster de acordo com o geojson do talhão, e atribui 255 aos valores "nodata"
                
                # Calcula a moda do raster clipado, ignorando o valor "nodata"
                moda = scipy.stats.mode(raster_out[(raster_out != 255) & (raster_out != -1)]).mode
                
                # Calcula os valores únicos do raster clipado
                valores_unicos = np.unique(raster_out).tolist()
                classes_possiveis_str = ""
                for valor in valores_unicos:
                    if len(classes_possiveis_str) == 0:
                        if valor in dict_classes:
                            classes_possiveis_str += dict_classes[valor]
                    else:
                        classes_possiveis_str += ", "
                        classes_possiveis_str += dict_classes[valor]
                gdf_out.loc[idx_talhao, "classes_possiveis"] = classes_possiveis_str

                # Preenche o gdf de saída
                if not np.isnan(moda):
                    classe_raster = dict_classes[moda]        # Obtém a classe a partir da moda
                    gdf_out.loc[idx_talhao, column_name] = classe_raster
                else:
                    print("Algo deu errado no cálculo da moda.")
                    gdf_out.loc[idx_talhao, column_name] = "ERRO_MODA"

        # Apaga arquivos da memória
        del raster, gdf_copy

        # Gdf de saída
        return gdf_out

    print("Executando preenche_atributos_raster")

    # IRRIGAÇÃO
    if irrigation_raster_in_path != None:
        print("Irrigação")

        # Dicionário de classes
        irrigation_dict_classes = {0:"NO", 1:"YES", 2:"YES", 3:"YES", 255:"NODATA"}

        # Análise raster e preenchimento
        gdf_in = analise_raster(gdf_out=gdf_in, raster_in_path=irrigation_raster_in_path, dict_classes=irrigation_dict_classes, column_name="irrigation")

    # LAND COVER
    if lulc_raster_in_path != None:
        print("Land use/ Land cover")

        # Dicionário de classes
        if lulc_origem_dict == "mapbiomas":
            lulc_dict_classes = {1: 'floresta', 3: 'formação florestal', 4: 'formação savânica',
                            5: 'mangue', 6: 'floresta alagável (beta)', 49: 'restinga arbórea',
                            10: 'formação natural não florestal', 11: 'campo alagado e área pantanosa',
                            12: 'formação campestre', 32: 'apicum', 29: 'afloramento rochoso', 50: 'restinga herbácea',
                            13: 'outras formações não florestais', 14: 'agropecuária', 15: 'pastagem',
                            18: 'agricultura', 19: 'lavoura temporária', 39: 'soja', 20: 'cana', 40: 'arroz',
                            62: 'algodão (beta)', 41: 'outras lavouras temporárias', 36: 'lavoura perene',
                            46: 'café', 47: 'citrus', 35: 'dendê (beta)', 48: 'outras lavouras perenes',
                            9: 'silvicultura', 21: 'mosaico de usos', 22: 'área não vegetada', 23: 'praia, duna e areal',
                            24: 'área urbanizada', 30: 'mineração', 25: 'outras áreas não vegetadas', 26: "corpo d'água",
                            33: 'rio, lago e oceano', 31: 'aquicultura', 27: 'não observado', 255:'nodata'}
        elif lulc_origem_dict == "agrosatelite":
            lulc_dict_classes = {1:'soja', 2:'milho', 3:'algodão', 4:'cana', 5:'outras culturas temporárias',
                            6:'culturas permanentes', 7:'pastagem', 8:'floresta nativa', 9:'vegetação natural não florestal',
                            10:'silvicultura', 11:'outros (infraestrutura, água)', 12:'áreas ágricolas sem mapeamento da cultura', 255:'nodata'}
        elif lulc_origem_dict == "simfaz":
            lulc_dict_classes = {201:"soja", 202:"algodão", 203:"milho", 204:"arroz", 205:"cana", 206:"café", 207:"citrus", 208:"área urbana", 101:"água", 209:"pastagem", 210:"silvicultura", 221:"outros usos antrópicos", 102:"natural florestal", 103:"natural não florestal", 222:"outros usos", 255:'nodata'}
        
        # Análise do raster com preenchimento das informações
        gdf_in = analise_raster(gdf_out=gdf_in, raster_in_path=lulc_raster_in_path, dict_classes=lulc_dict_classes, column_name="class")

    # Gdf de saída
    return gdf_in



################################################################################
# Localiza estradas pelo OpenStreetMap
def busca_estradas(gdf_in, roads_in=None, is_wgs=True):
    """
    gdf_in: GeoDataFrame de entrada (que será preenchido)
    is_wgs: indica se o(s) dado(s) de entrada está(ão) em wgs84 (booleano)
    OBS: os dados de entrada precisam estar no mesmo sistema de referência de coordenadas
    """

    print("Executando busca_estradas")
    
    # Cópia do gdf para gerar buffer de cada talhão
    gdf_copy = gdf_in.copy()

    # Se a entrada estiver em grau, precisa reprojetar para gerar o buffer
    if is_wgs:
        gdf_copy = grau_para_utm(gdf_copy)    # Para gerar o buffer em metros precisa estar em utm
    
    # Gerando buffer de 10Km e reprojetando para wgs84 (src compatível com o osmnx)
    geom_dissolve_buffer = gdf_copy.buffer(10000).to_crs("EPSG:4326").unary_union
    geom_dissolve_fazenda = gdf_copy.buffer(45).to_crs("EPSG:4326").unary_union     # aplicando buffer para corrigir geometrias e ajudar com intersects de estradas próximas

    # Se não for passado um geodataframe com os dados de estradas, vai procurar no OSM
    if roads_in is None:       
        # Verificando se a geometria resultante é válida
        if not geom_dissolve_fazenda.is_valid:
            print("A geometria corrigida ainda é inválida.")
            gdf_in["paved_road"] = "NULL"
        
        else:
            # Chamada api OSM
            gdf_estradas_osm = ox.features_from_polygon(geom_dissolve_buffer, tags={"highway":True}).reset_index(drop=True)
            
            try:
                # Filtragem de estradas com pavimentação
                try:
                    gdf_estradas_osm_filtrada = gdf_estradas_osm[["ref", "surface", "geometry"]]
                    gdf_estradas_osm_filtrada = gdf_estradas_osm_filtrada[gdf_estradas_osm_filtrada["geometry"].geom_type != "Point"].reset_index(drop=True)
                    gdf_estradas_osm_filtrada = gdf_estradas_osm_filtrada[((gdf_estradas_osm_filtrada["surface"].isin(["paved", "asphalt"])) | (gdf_estradas_osm_filtrada["ref"].str.contains("BR|AC|AL|AM|AP|BA|CE|DF|ES|GO|MA|MG|MS|MT|PA|PB|PE|PI|PR|RJ|RN|RO|RR|RS|SC|SE|SP|TO", case=False))) & (gdf_estradas_osm_filtrada["surface"] !="unpaved")]   # que não sejam "unpaved" |sc|SC|pr|PR|rs|RS|sp|SP|mt|MT|df|DF|ac|AC|rj|RJ|mg|MG
                except Exception as e:
                    print(f"Erro {e}. Tratando.")
                    gdf_estradas_osm_filtrada = gdf_estradas_osm[["surface", "geometry"]]
                    gdf_estradas_osm_filtrada = gdf_estradas_osm_filtrada[gdf_estradas_osm_filtrada["geometry"].geom_type != "Point"].reset_index(drop=True)
                    gdf_estradas_osm_filtrada = gdf_estradas_osm_filtrada[gdf_estradas_osm_filtrada["surface"].isin(["paved", "asphalt"])]   # que não sejam "unpaved"
            
            except Exception as e:
                print(f"Erro {e}. Tratando.")
                gdf_estradas_osm_filtrada = gdf_estradas_osm

            # Verifica se tem ao menos uma estrada pavimentada a 10Km do buffer dos talhões dissolvidos
            if len(gdf_estradas_osm_filtrada) > 0:
                # Dissolve das estradas
                geom_estradas_dissolve = gdf_estradas_osm_filtrada.unary_union.intersection(geom_dissolve_buffer)

                # Preenche a informação de todos os talhões conforme o relacionamento da união das geometrias com as estradas
                if (geom_dissolve_fazenda.intersects(geom_estradas_dissolve)):     #  | (geom_dissolve_fazenda.touches(geom_estradas_dissolve)) | (geom_dissolve_fazenda.overlaps(geom_estradas_dissolve))
                    gdf_in["paved_road"] = "TOUCH_ROAD"
                    print("TOUCH_ROAD")
                else:
                    gdf_in["paved_road"] = "10KM_ROAD"
                    print("10KM_ROAD")

            # Se não houver nenhuma, preenche todas com "NO"
            else:
                gdf_in["paved_road"] = "NO"
                print("NO")

    # Se for passado um geodataframe com as estradas
    else:
        #### AINDA NÃO ESTÁ IMPLEMENTADO!!!!!!!!!!
        if len(roads_in) > 0:
            # Passa para utm se a entrada for wgs
            if is_wgs:
                roads_in = grau_para_utm(roads_in)
            
            # Geometrias para as operações
            geom_dissolve_buffer = gdf_copy.buffer(10000).unary_union
            geom_dissolve_fazenda = gdf_copy.unary_union
            roads_in = roads_in[roads_in.intersects(geom_dissolve_fazenda)]

            # Verificando os intersects
            if len(roads_in) > 0:
                if roads_in.intersects(geom_dissolve_fazenda).sum() > 0 | roads_in.touches(geom_dissolve_fazenda).sum() > 0 | roads_in.overlaps(geom_dissolve_fazenda).sum() > 0:
                    gdf_in["paved_road"] = "TOUCH_ROAD"
                    print("TOUCH_ROAD")
                else:
                    gdf_in["paved_road"] = "10KM_ROAD"
                    print("10KM_ROAD")
            else:
                gdf_in["paved_road"] = "NO"
                print("NO")
        
        else:
            gdf_in["paved_road"] = "NO"

    # Saída gdf preenchido
    return gdf_in




##############################################################
# Imprime tabelas com os resultados por classe
def analisa_resultados(analistas, mapbiomas, simfaz, agrosatelite):
    """
    Imprime na tela tabelas-resumo da taxa de acerto do algoritmo conforme a fonte de dados
    Recebe quatro geodataframes, na ordem acima
    """

    print("\nExecutando analisa_resultados...\n")

    # # Filtrar colunas de interesse
    analistas = analistas[["id", "class", "irrigation", "paved_road"]]
    mapbiomas = mapbiomas[["id", "class", "irrigation", "paved_road"]]
    simfaz = simfaz[["id", "class", "irrigation", "paved_road"]]
    agrosatelite = agrosatelite[["id", "class", "irrigation", "paved_road"]]

    print("Contagem de classes do arquivo analistas")
    print(analistas["class"].value_counts())
    print()
    print("Contagem de classes do arquivo mapbiomas:")
    print(mapbiomas["class"].value_counts())
    print()
    print("Contagem de classes do arquivo simfaz:")
    print(simfaz["class"].value_counts())
    print()
    print("Contagem de classes do arquivo agrosatélite:")
    print(agrosatelite["class"].value_counts())
    print()

    # Join dos dados
    join = analistas.join(mapbiomas, on="id", rsuffix="_mapbiomas",).join(simfaz, on="id", rsuffix="_simfaz").join(agrosatelite, on="id", rsuffix="_agrosatelite")
    join = join.drop(columns=["id_mapbiomas", "id_simfaz", "id_agrosatelite"])

    # Padronização de dados
    dict_classes = {"DIRTY_PASTURE":"PASTURE",
                    "CLEAN_PASTURE":"PASTURE",
                    "WATER":"OTHER",
                    "REGENERATION":"NATIVE_VEGETATION",
                    "pastagem":"PASTURE",
                    "formação florestal":"NATIVE_VEGETATION",
                    "lavoura temporária":"ANNUAL_CROPS",
                    "mosaico de usos":"PRIVATE_INFRASTRUCTURE",
                    "formação savânica":"NATIVE_VEGETATION",
                    "rio, lago e oceano": "OTHER",
                    "silvicultura":"SILVICULTURE",
                    "outras áreas não vegetadas":"PRIVATE_INFRASTRUCTURE",
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
                    "outros usos antrópicos":"PRIVATE_INFRASTRUCTURE",
                    "área urbana":"PRIVATE_INFRASTRUCTURE",
                    "outros (infraestrutura, água)": "OTHER",
                    "vegetação natural não florestal":"NATIVE_VEGETATION",
                    "floresta nativa":"NATIVE_VEGETATION",
                    "outras culturas temporárias":"ANNUAL_CROPS",
                    "culturas permanentes":"PERENNIAL_CROPS",
                    None:"NULL"
                }

    join = join.replace(dict_classes)

    # # Print das classes de cada fonte, para conferir se a substituição foi feita corretamente
    # print(join["class"].value_counts())
    # print()
    # print(join["class_mapbiomas"].value_counts())
    # print()
    # print(join["class_agrosatelite"].value_counts())
    # print()
    # print(join["class_simfaz"].value_counts())

    # Comparar colunas
    colunas_a_comparar = [('class_mapbiomas', 'class'),
        ('irrigation_mapbiomas', 'irrigation'),
        ('paved_road_mapbiomas', 'paved_road'),
        ('class_simfaz', 'class'),
        ('irrigation_simfaz', 'irrigation'),
        ('paved_road_simfaz', 'paved_road'),
        ('class_agrosatelite', 'class'),
        ('irrigation_agrosatelite', 'irrigation'),
        ('paved_road_agrosatelite', 'paved_road')]

    resultados_class = {}
    resultados_irrigation = {}
    resultados_paved_road = {}
    for col_pred, col_true in colunas_a_comparar:
        matches = join[col_pred] == join[col_true]
        precisao = matches.mean() * 100
        absolute_matches = matches.sum()
        if "class" in col_pred:
            resultados_class[col_pred] = {'Taxa de acerto (porcentagem)': precisao, 'Número de acertos': absolute_matches}
        elif "irrigation" in col_pred:
            resultados_irrigation[col_pred] = {'Taxa de acerto (porcentagem)': precisao, 'Número de acertos': absolute_matches}
        elif "paved_road" in col_pred:
            resultados_paved_road[col_pred] = {'Taxa de acerto (porcentagem)': precisao, 'Número de acertos': absolute_matches}

    # Converter o dict para dataframe para visualizar os resultados em tabela
    resultados_class_df = pd.DataFrame(resultados_class).T
    resultados_paved_road_df = pd.DataFrame(resultados_paved_road).T
    resultados_irrigation_df = pd.DataFrame(resultados_irrigation).T
    print("\nTaxa de acertos por raster landcover (em porcentagem)")
    print("Classes:")
    print(resultados_class_df)
    print()
    print("Rodovias:")
    print(resultados_paved_road_df)
    print()
    print("Irrigação:")
    print(resultados_irrigation_df)
    print()

    # Comparar por classes
    taxa_acerto_por_classe = {}
    for classe in join['class'].unique():
        subset = join[join['class'] == classe]
        for col_pred, col_true in colunas_a_comparar:
            fonte = col_pred
            matches = subset[col_pred] == subset[col_true]
            precisao = matches.mean() * 100
            absolute_matches = matches.sum()
            if "class_" in fonte:
                if classe not in taxa_acerto_por_classe:
                    taxa_acerto_por_classe[classe] = {}
                taxa_acerto_por_classe[classe][fonte] = precisao

    print("\nTaxa de acertos por classe (em porcentagem)")
    df_taxa_por_classe = pd.DataFrame(taxa_acerto_por_classe).T
    print(df_taxa_por_classe)