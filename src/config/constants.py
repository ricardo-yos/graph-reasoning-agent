"""
Graph Reasoning Agent Constants
===============================

This module defines project-wide constants for the Graph Reasoning Agent,
including configurations and reference data used throughout the system.

It contains:
- OpenStreetMap (OSM) feature layers and tag filters for geographic extraction
- SIDRA table codes, descriptive mappings, and data collection years
- Neighborhood codes for Santo André
- Key variables for demographic and income analysis
- Valid combinations of neighborhoods and SIDRA tables
- Graph node and relationship structure information for question answering
"""

# OSM feature layers to extract with corresponding tag filters
LAYERS_TO_EXTRACT = [
    ("residential_areas", {"landuse": "residential"}),
    ("apartments", {"building": "apartments"}),
    ("houses", {"building": "house"}),
    ("parks", {"leisure": "park"}),
    ("supermarkets", {"shop": "supermarket"}),
    ("bus_stations", {"amenity": "bus_station"})
]

# SIDRA table IDs to download data from
TABLES = [
    "185",  # Total private permanent households (2010)
    "1383", # Literacy rate (2010)
    "3324", # Total literate population (2010)
    "202",  # Total resident population (2010)
    "3170", # Population with income and average monthly income (2010)
    "9922", # Total private permanent households (2022)
    "9922"  # Total resident population (2022)
]

# Neighborhood codes for Santo André
NEIGHBORHOODS = [
    "3547809001",  # Tamanduateí 2
    "3547809002",  # Vila Metalúrgica
    "3547809003",  # Vila Camilópolis
    "3547809004",  # Jardim Utinga
    "3547809005",  # Jardim das Maravilhas
    "3547809006",  # Vila Lucinda
    "3547809007",  # Jardim Santo Antônio
    "3547809008",  # Vila Francisco Matarazzo
    "3547809009",  # Parque Oratório
    "3547809010",  # Parque das Nações
    "3547809011",  # Santa Terezinha
    "3547809012",  # Tamanduateí 4
    "3547809013",  # Tamanduateí 1
    "3547809014",  # Campestre
    "3547809015",  # Tamanduateí 3
    "3547809016",  # Bangú
    "3547809017",  # Tamanduateí 5
    "3547809018",  # Jardim
    "3547809019",  # Santa Maria
    "3547809020",  # Vila Palmares
    "3547809021",  # Vila Sacadura Cabral
    "3547809022",  # Vila Alpina
    "3547809023",  # Vila Guiomar
    "3547809024",  # Centro
    "3547809025",  # Casa Branca
    "3547809026",  # Tamanduateí 7
    "3547809027",  # Parque Marajoara
    "3547809028",  # Vila Homero Thon
    "3547809029",  # Vila América
    "3547809030",  # Bairro Silveira
    "3547809031",  # Vila Alzira
    "3547809032",  # Vila Assunção
    "3547809033",  # Vila Bastos
    "3547809034",  # Jardim Bela Vista
    "3547809035",  # Vila Alice
    "3547809036",  # Vila Príncipe de Gales
    "3547809037",  # Vila Valparaíso
    "3547809038",  # Vila Floresta
    "3547809039",  # Vila Gilda
    "3547809040",  # Paraíso
    "3547809041",  # Vila Pires
    "3547809042",  # Vila Humaitá
    "3547809043",  # Vila Guarani
    "3547809044",  # Jardim Marek
    "3547809045",  # Cidade São Jorge
    "3547809046",  # Parque Gerassi
    "3547809047",  # Vila Progresso
    "3547809048",  # Vila Helena
    "3547809049",  # Vila Scarpelli
    "3547809050",  # Jardim Bom Pastor
    "3547809051",  # Jardim Stella
    "3547809052",  # Jardim Cristiane
    "3547809053",  # Vila Linda
    "3547809054",  # Vila Junqueira
    "3547809055",  # Jardim Ipanema
    "3547809056",  # Vila Guaraciaba
    "3547809057",  # Condomínio Maracanã
    "3547809058",  # Vila Tibiriçá
    "3547809059",  # Vila Suíça
    "3547809060",  # Vila Lutécia
    "3547809061",  # Jardim Santa Cristina
    "3547809062",  # Jardim do Estádio
    "3547809063",  # Jardim Alvorada
    "3547809064",  # Jardim Las Vegas
    "3547809065",  # Sítio dos Vianas
    "3547809066",  # Vila Luzita
    "3547809067",  # Jardim Santo André
    "3547809068",  # Parque dos Pássaros
    "3547809069",  # Jardim Irene
    "3547809070",  # Jardim João Ramalho
    "3547809071",  # Cata Preta
    "3547809072",  # Parque do Pedroso
    "3547809073",  # Recreio da Borda do Campo
    "3547809074",  # Três Divisas
    "3547809075",  # Parque Miami
    "3547809076",  # Jardim Riviera
    "3547809077",  # Waisberg I
    "3547809078",  # Waisberg II
    "3547809079",  # Sítio dos Teco
    "3547809080",  # Parque Represa Billings II
    "3547809081",  # Jardim Clube de Campo
    "3547809082",  # Sítio Taquaral
    "3547809083",  # Parque Represa Billings III
    "3547809084",  # Parque Novo Oratório
    "3547809085",  # Jardim Santo Alberto
    "3547809086",  # Jardim Ana Maria
    "3547809087",  # Pólo Petroquímico de Capuava
    "3547809088",  # Jardim Itapoan
    "3547809089",  # Parque Capuava
    "3547809090",  # Parque Erasmo Assunção
    "3547809091",  # Vila Curuçá
    "3547809092",  # Parque Jaçatuba
    "3547809093",  # Parque João Ramalho
    "3547809094",  # Jardim Rina
    "3547809095",  # Jardim Alzira Franco
    "3547809096",  # Tamanduateí 8
    "3547809097",  # Tamanduateí 6
    "3547809098",  # Acampamento Anchieta
    "3547809099",  # Jardim Guaripocaba
    "3547809100",  # Parque das Garças
    "3547809101",  # Parque Rio Grande
    "3547809102",  # Cabeceiras do Rio Pequeno
    "3547809103",  # Parque América
    "3547809104",  # Rio Grande
    "3547809105",  # Cabeceiras do Rio Mogi
    "3547809106",  # Campo Grande
    "3547809107",  # Estância Rio Grande
    "3547809108",  # Jardim Joaquim Eugênio de Lima
    "3547809109",  # Cabeceiras do Araçaúva
    "3547809110",  # Várzea do Rio Grande
    "3547809111",  # Reserva Biológica Alto da Serra
    "3547809112",  # Parque Estadual da Serra do Mar
    "3547809113",  # Paranapiacaba
    "3547809114",  # Cabeceiras do Rio Grande
    "3547809115",  # Jardim Cipreste
    "3547809116",  # Jardim Guarará
    "3547809117",  # Jardim Jamaica
    "3547809118",  # Jardim Santo André CDHU
    "3547809119",  # Jardim Telles de Menezes
    "3547809120",  # Jardim Vila Rica
    "3547809121",  # Novo Homero Thon
    "3547809122",  # Pinheirinho
    "3547809123",  # Vila Aquilino
    "3547809124",  # Vila João Ramalho
    "3547809125",  # Vila Vitória
    "3547809126",  # Centreville
    "3547809127",  # Miami / Riviera
    "3547809128",  # Rio Bonito
    "3547809129",  # Rio Mogi
    "3547809130",  # Araçaúva
    "3547809131",  # Rio Pequeno
    "3547809132",  # Várzea do Tamanduateí
    "3547809500",  # Silveira
]

# Desired variables for analysis
DESIRED_VARIABLES = [
    "Pessoas de 10 anos ou mais de idade, com rendimento (Pessoas)",
    "Valor do rendimento nominal médio mensal das pessoas de 10 anos ou mais de idade, com rendimento (Reais)"
]

# Manually validated valid neighborhood-table combinations on SIDRA
VALID_COMBINATIONS = {
    "3547809019": ["185", "1383", "3324", "202", "3170"],
    "3547809074": [],
    "3547809077": [],
    "3547809078": [],
    "3547809087": [],
    "3547809097": [],
    "3547809102": [],
    "3547809104": [],
    "3547809105": [],
    "3547809109": [],
    "3547809112": [],
    "3547809114": [],
    "3547809115": ["9922", "9923"],
    "3547809116": ["9922", "9923"],
    "3547809117": ["9922", "9923"],
    "3547809118": ["9922", "9923"],
    "3547809119": ["9922", "9923"],
    "3547809120": ["9922", "9923"],
    "3547809121": ["9922", "9923"],
    "3547809122": ["9922", "9923"],
    "3547809123": ["9922", "9923"],
    "3547809124": ["9922", "9923"],
    "3547809125": ["9922", "9923"],
    "3547809126": ["9922", "9923"],
    "3547809127": ["9922", "9923"],
    "3547809128": ["9922", "9923"],
    "3547809129": ["9922", "9923"],
    "3547809130": ["9922", "9923"],
    "3547809131": [],
    "3547809132": ["9922", "9923"],
    "3547809500": ["9922", "9923"],
    # Add all other valid combinations here
}

# Table ID to descriptive name mapping for CSV readability
TABLE_ID_TO_NAME = {
    "185": "Total_Private_Households",
    "9922": "Total_Private_Households",
    "202": "Total_Resident_Population",
    "9923": "Total_Resident_Population",
    "1383": "Literacy_Rate",
    "3324": "Total_Literate_Population",
    "3170_1": "Population_with_Income",
    "3170_2": "Average_Monthly_Income", 
    # Add more mappings as needed
}

# Table ID to data collection year
TABLE_ID_TO_YEAR = {
    "185": "2010",
    "9922": "2022",
    "202": "2010",
    "9923": "2022", 
    "1383": "2010",
    "3324": "2010",
    "3170_1": "2010",
    "3170_2": "2010",
    # Add more mappings as needed
}

# Graph structure description for question generation
GRAPH_INFO = """
Nodes:
- Neighborhood: { name, area_km2, centroid_lat, centroid_lon, neighborhood_id, average_monthly_income, literacy_rate, population_with_income, total_literate_population, total_private_households, total_resident_population }
- Place: { name, place_id, rating, type, latitude, longitude, num_reviews } where type ∈ {'pet_store', 'veterinary_care'}
- Road: { name, highway, oneway, length, maxspeed, osmid, road_id, u, v }
- Intersection: { highway, osmid, lat, lon, street_count }
- Review: { rating, review_id, author, text, date }

Relationships:
- (Neighborhood)-[:CONTAINS]->(Place)
- (Neighborhood)-[:CONTAINS]->(Road)
- (Road)-[:CONTAINS]->(Place)
- (Intersection)-[:ROAD]->(Intersection)
- (Place)-[:NEAR]->(Intersection)
- (Place)-[:HAS_REVIEW]->(Review)
"""
