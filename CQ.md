# Competency Questions (CQs), SPARQL queries, and expected answers

### **CQ1: What types of credentials does OPCUAServer1, with URL `opc.tcp://127.0.0.1:4840/server1/`, support?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>

SELECT ?type
WHERE {
  ?conn core:hasURL "opc.tcp://127.0.0.1:4840/server1/"^^xsd:anyURI .
  ?conn core:hasCredentials ?cred .
  ?cred rdf:type ?type .
  ?type rdfs:subClassOf core:Credentials .
}
```

**Answer**
```txt
core:UserPassCredentials
core:CertCredentials
```

---

### **CQ2: What SecurityPolicy and what MessageSecurityMode does OPCUAServer1 utilize?**

**SPARQL**
```sparql
PREFIX opcua: <http://example.org/sdo/opcua#>
PREFIX : <http://example.org/sdo/abox#>

SELECT ?policy ?mode
WHERE {
  :OpcUaConnection1 opcua:hasMessageSecurity ?msec .
  ?msec opcua:hasSecurityPolicy ?policy .
  ?msec opcua:hasMode ?mode .
}
```

**Answer**
```txt
"Aes128Sha256RsaOaep"; "SignAndEncrypt"^^opcua:mode
```

---

### **CQ3: What password is required to authenticate the user "admin" on OPCUAServer1?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#> 
PREFIX : <http://example.org/sdo/abox#>

SELECT ?password
WHERE {
  :OpcUaConnection1 core:hasCredentials ?cred .
  ?cred core:hasUsername "admin"^^xsd:string.
  ?cred core:hasPassword ?password .
}
```

**Answer**
```txt
"PWD@server1"
```

---

### **CQ4: Where are the certificate and private key located used for authentication on OPCUAServer1?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>
PREFIX : <http://example.org/sdo/abox#>

SELECT ?cert ?pk
WHERE {
  :OpcUaConnection1 core:hasCredentials ?cred .
  ?cred core:hasCertURI ?cert .
  ?cred core:hasPrivateKeyURI ?pk .
}
```

**Answer**
```txt
"./certificates/client-certificate.der"^^xsd:anyURI
"./certificates/client-private-key.pem"^^xsd:anyURI
```

---

### **CQ5: What is the access token utilized for authentication on the InfluxDB server with URL `http://127.0.0.1:8086`?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>

SELECT ?token
WHERE {
  ?conn core:hasURL "http://127.0.0.1:8086"^^xsd:anyURI .
  ?conn core:hasCredentials ?cred .
  ?cred core:hasToken ?token .
}
```

**Answer**
```txt
"RDHByh4KFzx5ESBzebaP9UzhSHd2FvQseZvypwTJWUNFG_hfshHclChF9o-dJSF0EeexqwkAZcTwr5Kr6gAHA=="
```

---

### **CQ6: What URLs correspond to OPC UA servers which support username and password authentication?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>

SELECT ?url
WHERE {
  ?cred rdf:type core:UserPassCredentials .
  ?conn core:hasCredentials ?cred .
  ?conn core:hasURL ?url .
}
```

**Answer**
```txt
"opc.tcp://127.0.0.1:4840/server1/"^^xsd:anyURI
"opc.tcp://127.0.0.1:4840/server2/"^^xsd:anyURI
```

---

### **CQ7: Which SignalDescriptions provide historical data via i) OPC UA, ii) InfluxDB, iii) or provide no historical data at all?**

**i) SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>
PREFIX opcua: <http://example.org/sdo/opcua#>

SELECT ?sd
WHERE {
  ?sd rdf:type core:SignalDescription .
  ?sd core:hasHistAccess ?da .
  ?da rdf:type opcua:OpcUaAccess .
}
```

**Answer**
```txt
:Line2_UnitsPerBatch
:Press_MaterialTemperature
:Press_Pressure
:Cutter_BladeSpeed
:Cutter_Pressure
```

**ii) SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>
PREFIX influxdb: <http://example.org/sdo/influxdb#>

SELECT ?sd
WHERE {
  ?sd rdf:type core:SignalDescription .
  ?sd core:hasHistAccess ?da .
  ?da rdf:type influxdb:InfluxDbAccess .
}
```

**Answer**
```txt
:Room1_Temperature
:Extruder_Temperature
:Extruder_Throughput
:Press_MaterialTemperature
:Press_Pressure
:Trimmer_Pressure
:Trimmer_ZPosition
```

**iii) SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>

SELECT ?sd
WHERE {
  ?sd rdf:type core:SignalDescription .
  FILTER NOT EXISTS { ?sd core:hasHistAccess ?da . }
}
```

**Answer**
```txt
:Belt_Speed
:Trimmer_Speed
:Centrifuge_Speed
:Centrifuge_Acceleration
```

---

### **CQ8: Which SignalDescriptions provide near real-time data via OPC UA?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>
PREFIX opcua: <http://example.org/sdo/opcua#>

SELECT ?sd
WHERE {
  ?sd rdf:type core:SignalDescription .
  ?sd core:hasNrtAccess ?da .
  ?da rdf:type opcua:OpcUaAccess .
}
```

**Answer**
```txt
:Line2_UnitsPerBatch
:Extruder_Temperature
:Extruder_Throughput
:Press_MaterialTemperature
:Press_Pressure
:Cutter_BladeSpeed
:Cutter_Pressure
:Belt_Speed
:Centrifuge_Speed
:Centrifuge_Acceleration
```

---

### **CQ9: What are the SignalTypes of the Trimmer's SignalDescriptions?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>
PREFIX : <http://example.org/sdo/abox#>

SELECT ?sd ?st
WHERE {
  :Trimmer core:hasSignalDescription ?sd .
  ?sd core:hasSignalType ?st .
}
```

**Answer**
```txt
:Trimmer_Speed; LinearInterpolation^^core:signalType
:Trimmer_Pressure; HoldObservations^^core:signalType
:Trimmer_ZPosition; Observations^^core:signalType
```

---

### **CQ10: What is the unit and the datatype of UnitsPerBatch?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>
PREFIX : <http://example.org/sdo/abox#>

SELECT ?unit ?dt
WHERE {
  :Line2_UnitsPerBatch core:hasUnit ?unit .
  :Line2_UnitsPerBatch core:hasDatatype ?dt .
}
```

**Answer**
```txt
unit:ONE; xsd:integer
```

---

### **CQ11: What metadata belongs to the Press's MaterialTemperature?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>
PREFIX : <http://example.org/sdo/abox#>

SELECT ?key ?value
WHERE {
  :Press_MaterialTemperature core:hasMetaData ?md .
  ?md core:hasKey ?key .
  ?md core:hasValue ?value .
}
```

**Answer**
```txt
"Min"; "150.0"^^xsd:double
"Max"; "250.0"^^xsd:double
```

---

### **CQ12: What are the key-value pairs of the InfluxDB tags used to address the Press's Pressure?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#> 
PREFIX influxdb: <http://example.org/sdo/influxdb#>
PREFIX : <http://example.org/sdo/abox#>

SELECT ?key ?value
WHERE {
  :Press_Pressure core:hasHistAccess ?access .
  ?access rdf:type influxdb:InfluxDbAccess .
  ?access influxdb:hasTag ?tag .
  ?tag core:hasKey ?key .
  ?tag core:hasValue ?value .
}
```

**Answer**
```txt
"Factory"; "VIE"
"Room"; "Room1"
"Line"; "Line1"
"Machine"; "Press"
```

---

### **CQ13: What top level assets exist?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>

SELECT ?top
WHERE {
  ?top rdf:type core:Asset .
  FILTER NOT EXISTS { ?parent core:hasAsset ?top . }
}
```

**Answer**
```txt
:FactoryVIE
:Centrifuge
```

---

### **CQ14: What leaf assets exist?**

**SPARQL**
```sparql
PREFIX core: <http://example.org/sdo/core#>

SELECT ?leaf
WHERE {
  ?parent core:hasAsset ?leaf .
  FILTER NOT EXISTS { ?leaf core:hasAsset ?child . }
}
```

**Answer**
```txt
:Cutter
:Extruder
:Press
:Belt
:Trimmer
```
