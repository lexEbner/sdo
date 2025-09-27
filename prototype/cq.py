import asyncio

from asyncua import Client, ua
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from rdflib import Graph
from datetime import datetime, timedelta

def cq1(g):
    print('CQ1: What types of credentials does OPCUAServer1, with URL "opc.tcp://127.0.0.1:4840/server1/", support?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>

SELECT ?type
WHERE {
  ?conn core:hasURL "opc.tcp://127.0.0.1:4840/server1/"^^xsd:anyURI .
  ?conn core:hasCredentials ?cred .
  ?cred rdf:type ?type .
  ?type rdfs:subClassOf core:Credentials .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["type"])

def cq2(g):
    print('CQ2: What SecurityPolicy and what MessageSecurityMode does OPCUAServer1 utilize?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX opcua: <https://purl.org/signal-description-ontology/opcua#>
PREFIX : <https://purl.org/signal-description-ontology/abox#>

SELECT ?policy ?mode
WHERE {
  :OpcUaConnection1 opcua:hasMessageSecurity ?msec .
  ?msec opcua:hasSecurityPolicy ?policy .
  ?msec opcua:hasMode ?mode .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row)

def cq3(g):
    print('CQ3: What password is required to authenticate the user "admin" on OPCUAServer1?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#> 
PREFIX : <https://purl.org/signal-description-ontology/abox#>

SELECT ?password
WHERE {
  :OpcUaConnection1 core:hasCredentials ?cred .
  ?cred core:hasUsername "admin"^^xsd:string.
  ?cred core:hasPassword ?password .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["password"])

def cq4(g):
    print('CQ4: Where are the certificate and private key located used for authentication on OPCUAServer1?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>
PREFIX : <https://purl.org/signal-description-ontology/abox#>

SELECT ?cert ?pk
WHERE {
  :OpcUaConnection1 core:hasCredentials ?cred .
  ?cred core:hasCertURI ?cert .
  ?cred core:hasPrivateKeyURI ?pk .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row)

def cq5(g):
    print('CQ5: What is the access token utilized for authentication on the InfluxDB server with URL "http://127.0.0.1:8086"?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>

SELECT ?token
WHERE {
  ?conn core:hasURL "http://127.0.0.1:8086"^^xsd:anyURI .
  ?conn core:hasCredentials ?cred .
  ?cred core:hasToken ?token .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["token"])

def cq6(g):
    print('CQ6: What URLs correspond to OPC UA servers which support username and password authentication?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>

SELECT ?url
WHERE {
  ?cred rdf:type core:UserPassCredentials .
  ?conn core:hasCredentials ?cred .
  ?conn core:hasURL ?url .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["url"])

def cq7(g):
    print('CQ7: Which SignalDescriptions provide historical data via i) OPC UA, ii) InfluxDB, iii) or provide no historical data at all?')
    print('i)')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>
PREFIX opcua: <https://purl.org/signal-description-ontology/opcua#>

SELECT ?sd
WHERE {
  ?sd rdf:type core:SignalDescription .
  ?sd core:hasHistAccess ?da .
  ?da rdf:type opcua:OpcUaAccess .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["sd"])
    print("+++")

    print('ii)')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>
PREFIX influxdb: <https://purl.org/signal-description-ontology/influxdb#>

SELECT ?sd
WHERE {
  ?sd rdf:type core:SignalDescription .
  ?sd core:hasHistAccess ?da .
  ?da rdf:type influxdb:InfluxDbAccess .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["sd"])
    print("+++")

    print('iii)')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>

SELECT ?sd
WHERE {
  ?sd rdf:type core:SignalDescription .
  FILTER NOT EXISTS { ?sd core:hasHistAccess ?da . }
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["sd"])

def cq8(g):
    print('CQ8: Which SignalDescriptions provide near real-time data via OPC UA?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>
PREFIX opcua: <https://purl.org/signal-description-ontology/opcua#>

SELECT ?sd
WHERE {
  ?sd rdf:type core:SignalDescription .
  ?sd core:hasNrtAccess ?da .
  ?da rdf:type opcua:OpcUaAccess .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["sd"])

def cq9(g):
    print('CQ9: What are the SignalTypes of the Trimmer\'s SignalDescriptions?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>
PREFIX : <https://purl.org/signal-description-ontology/abox#>

SELECT ?sd ?st
WHERE {
  :Trimmer core:hasSignalDescription ?sd .
  ?sd core:hasSignalType ?st .
}
    """
    result = list(g.query(q))
    for row in result:
        print(row)

def cq10(g):
    print('CQ10: What is the unit and the datatype of UnitsPerBatch?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>
PREFIX : <https://purl.org/signal-description-ontology/abox#>

SELECT ?unit ?dt
WHERE {
  :Line2_UnitsPerBatch core:hasUnit ?unit .
  :Line2_UnitsPerBatch core:hasDatatype ?dt .
}
    """
    result = list(g.query(q))
    for row in result:
        print(f"{row["unit"]}; {row["dt"]}")

def cq11(g):
    print('CQ11: What metadata belongs to the Press\'s MaterialTemperature?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>
PREFIX : <https://purl.org/signal-description-ontology/abox#>

SELECT ?key ?value
WHERE {
  :Press_MaterialTemperature core:hasMetaData ?md .
  ?md core:hasKey ?key .
  ?md core:hasValue ?value .
}
    """
    result = list(g.query(q))
    for row in result:
        # print(f"{row["key"]}: {row["value"]}")
        print(row)

def cq12(g):
    print('CQ12: What are the key-value pairs of the InfluxDB tags used to address the Press\'s Pressure?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#> 
PREFIX influxdb: <https://purl.org/signal-description-ontology/influxdb#>
PREFIX : <https://purl.org/signal-description-ontology/abox#>

SELECT ?key ?value
WHERE {
  :Press_Pressure core:hasHistAccess ?access .
  ?access rdf:type influxdb:InfluxDbAccess .
  ?access influxdb:hasTag ?tag .
  ?tag core:hasKey ?key .
  ?tag core:hasValue ?value .
}
    """
    result = list(g.query(q))
    for row in result:
        print(f"{row["key"]}: {row["value"]}")

def cq13(g):
    print('CQ13: What top level assets exist?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>

SELECT ?top
WHERE {
  ?top rdf:type core:Asset .
  FILTER NOT EXISTS { ?parent core:hasAsset ?top . }
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["top"])

def cq14(g):
    print('CQ14: What leaf assets exist?')
    q = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX core: <https://purl.org/signal-description-ontology/core#>

SELECT ?leaf
WHERE {
  ?parent core:hasAsset ?leaf .
  FILTER NOT EXISTS { ?leaf core:hasAsset ?child . }
}
    """
    result = list(g.query(q))
    for row in result:
        print(row["leaf"])

# What SignalTypes can be assigned?
def cq15(g):
    # Traverse rdf:rest property and get rdf:first value
    q = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>

    SELECT ?value
    WHERE {
        core:hasSignalType rdfs:range ?range .
        ?range owl:oneOf ?list .
        ?list rdf:rest*/rdf:first ?value .
    }
    """
    result = list(g.query(q))
    for row in result:
        print(row)

# What MessageSecurityModes can be assigned?
def cq16(g):
    q = """
    PREFIX opcua: <https://purl.org/signal-description-ontology/opcua#>

    SELECT ?value
    WHERE {
        opcua:hasMode rdfs:range ?range .
        ?range owl:oneOf ?list .
        ?list rdf:rest*/rdf:first ?value .
    }
    """
    result = list(g.query(q))
    for row in result:
        print(row)

async def main():
    g = Graph()
    g.parse("onto_v01.ttl", format="turtle")
    g.parse("abox.ttl", format="turtle")

    cq1(g)
    print("-------------\n")
    cq2(g)
    print("-------------\n")
    cq3(g)
    print("-------------\n")
    cq4(g)
    print("-------------\n")
    cq5(g)
    print("-------------\n")
    cq6(g)
    print("-------------\n")
    cq7(g)
    print("-------------\n")
    cq8(g)
    print("-------------\n")
    cq9(g)
    print("-------------\n")
    cq10(g)
    print("-------------\n")
    cq11(g)
    print("-------------\n")
    cq12(g)
    print("-------------\n")
    cq13(g)
    print("-------------\n")
    cq14(g)
    print("-------------\n")
    # cq15(g)
    # print("-------------\n")
    # cq16(g)

if __name__ == "__main__":
    asyncio.run(main())