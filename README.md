# Signal Description Ontology

This repository contains an extensible ontology for semantic integration of time series data. 
It provides the ontology in different variants, example instance data and the prototype implementation to demonstrate source discovery and interoperability.

---

## Repository Structure

### Root Files

- **`onto_v08_with_comments.ttl`** 
  Full ontology including explanatory annotations using `rdf:comment`. 
- **`onto_v08.ttl`** 
  Full ontology without annotations. 
- **`abox.ttl`** 
  Example instance data illustrating the ontology's usage. 
- **`catalog-v001.xml`** 
  Configuration file enabling offline imports in Protégé. 

### Prototype

The **`prototype/`** folder contains scripts and data for simulation and evaluation:

- **`OpcUa_Server.py`** 
  OPC UA server for simulation. 
- **`WriteToInfluxDB.py`** 
  Script for simulation writing data to InfluxDB. 
- **`opcua_dump.txt`** 
  Historical OPC UA node data discovered via the ontology. 
- **`influx_dump.txt`** 
  InfluxDB point data discovered via the ontology. 
- **`Client.py`** 
  Client accessing the ontology to validate source discovery and interoperability, generating plots. 
- **`cq.py`** 
  Script for evaluating competency questions against the ontology. 

---

## Usage

### Running the Client

The `Client.py` script can be executed with parameters to either perform **source discovery** or **interpolation**:

```bash
python Client.py discover
```
Performs ontology-based source discovery on both InfluxDB and OPC UA sources.

```bash
python Client.py interpolation
```
Queries signals and applies interpolation, generating plots of the results.

---

## Tools

The ontology can be opened and explored in **Protégé**. 
Offline imports for opening `abox.ttl` are supported through the included `catalog-v001.xml` file. 

---

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
