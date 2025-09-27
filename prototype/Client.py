import sys
import json
import asyncio
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

from scipy.interpolate import interp1d
from datetime import datetime, timedelta

from asyncua import Client, ua
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from rdflib import Graph, XSD

##################################################################
# InfluxDB queries
##################################################################

def get_influxdb_connection(g, url):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX influxdb: <https://purl.org/signal-description-ontology/influxdb#>

    SELECT ?connection
    WHERE {{
        ?connection rdf:type influxdb:InfluxDbConnectionInfo.
        ?connection core:hasURL "{_url}"^^xsd:anyURI.
    }}
    """.format(_url=url)
    res = list(g.query(sparql_query))
    return res[0][0]


def get_token(g, connection):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>

    SELECT ?token
    WHERE {{
        <{_connection}> core:hasCredentials ?cred.
        ?cred core:hasToken ?token.
    }}
    """.format(_connection=connection)
    res = list(g.query(sparql_query))
    return res[0][0]


def get_bucket(g, influxdb_connection):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX influxdb: <https://purl.org/signal-description-ontology/influxdb#>

    SELECT ?bucket
    WHERE {{
        <{_influxdb_connection}> core:hasCredentials ?cred.
        ?conn influxdb:hasBucket ?bucket.
    }}
    """.format(_influxdb_connection=influxdb_connection)
    res = list(g.query(sparql_query))
    return res[0][0]


def get_organization(g, influxdb_connection):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX influxdb: <https://purl.org/signal-description-ontology/influxdb#>

    SELECT ?org
    WHERE {{
        <{_influxdb_connection}> core:hasCredentials ?cred.
        ?conn influxdb:hasOrganization ?org.
    }}
    """.format(_influxdb_connection=influxdb_connection)
    res = list(g.query(sparql_query))
    return res[0][0]


def get_signal_field_pairs(g, influxdb_connection, measurement):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX influxdb: <https://purl.org/signal-description-ontology/influxdb#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?signal ?field
    WHERE {{
        ?source influxdb:hasInfluxDbConnectionInfo <{_influxdb_connection}>.
        ?source influxdb:hasMeasurement "{_measurement}".
        ?source influxdb:hasField ?field.
        ?signal core:hasHistAccess ?source.
    }}
    """.format(_influxdb_connection=influxdb_connection, _measurement=measurement)
    res = list(g.query(sparql_query))
    return res


def get_influxdb_tags(g, signal):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX influxdb: <https://purl.org/signal-description-ontology/influxdb#>
    PREFIX : <https://purl.org/signal-description-ontology/abox#>

    SELECT ?key ?value
    WHERE {{
        {_signal} core:hasHistAccess ?source.
        ?source influxdb:hasTag ?tag.
        ?tag core:hasKey ?key.
        ?tag core:hasValue ?value.
    }}
    """.format(_signal=signal)
    res = list(g.query(sparql_query))
    return res

# generate Flux query

def create_flux_query(bucket, measurement, tags, field, start=-8):
    tag_str = ""
    for tag in tags:
        tag_str += f'r["{tag["key"]}"] == "{tag["value"]}" and '
    tag_str = tag_str[:-5]

    flux_query = """
    from(bucket: "{_bucket}")
     |> range(start: {_start})
     |> filter(fn: (r) => r["_measurement"] == "{_measurement}")
     |> filter(fn: (r) => {_tag_str})
     |> filter(fn: (r) => r["_field"] == "{_field}")
    """.format(_bucket=bucket, _measurement=measurement, _tag_str=tag_str, _field=field, _start=start)
    return flux_query

def get_influx_point(g, signal):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX influxdb: <https://purl.org/signal-description-ontology/influxdb#>
    PREFIX : <https://purl.org/signal-description-ontology/abox#>

    SELECT ?measurement ?field ?url ?bucket ?org ?stype
    WHERE {{
        {_signal} core:hasHistAccess ?access.
        {_signal} core:hasSignalType ?stype.
        ?access rdf:type influxdb:InfluxDbAccess.
        ?access influxdb:hasMeasurement ?measurement.
        ?access influxdb:hasField ?field.
        ?access influxdb:hasInfluxDbConnectionInfo ?connection.
        ?connection core:hasURL ?url.
        ?connection influxdb:hasBucket ?bucket.
        ?connection influxdb:hasOrganization ?org.
    }}
    """.format(_signal=signal)
    res = list(g.query(sparql_query))
    return res[0]

##################################################################
# OPC UA queries
##################################################################

def get_opcua_connection(g, url):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX opcua: <https://purl.org/signal-description-ontology/opcua#>

    SELECT ?connection
    WHERE {{
        ?connection rdf:type opcua:OpcUaConnectionInfo.
        ?connection core:hasURL "{_url}"^^xsd:anyURI.
    }}
    """.format(_url=url)
    res = list(g.query(sparql_query))
    return res[0][0]

def get_password(g, connection, username):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX opcua: <https://purl.org/signal-description-ontology/opcua#>

    SELECT ?password
    WHERE {{
        <{_connection}> core:hasCredentials ?cred.
        ?cred core:hasUsername "{_username}".
        ?cred core:hasPassword ?password.
    }}
    """.format(_connection=connection, _username=username)
    res = list(g.query(sparql_query))
    return res[0][0]

def get_hist_nodes(g, opcua_connection):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX opcua: <https://purl.org/signal-description-ontology/opcua#>

    SELECT ?asset_label ?signal_label ?uri ?id
    WHERE {{
        ?source opcua:hasOpcUaConnectionInfo <{_opcua_connection}>.
        ?source opcua:hasNamespaceURI ?uri.
        ?source opcua:hasIdentifier ?id.

        ?signal core:hasHistAccess ?source.
        ?signal rdfs:label ?signal_label.
        ?asset core:hasSignalDescription ?signal.
        ?asset rdfs:label ?asset_label.
    }}
    """.format(_opcua_connection=opcua_connection)
    return list(g.query(sparql_query))

# print OPC UA result

def print_datavalues(records):
    parsed_records = []
    for record in records:
        parsed_records.append({
            "Value": record.Value.Value,
            "VariantType" : record.Value.VariantType,
            "StatusCode" : str(record.StatusCode_),
            "SourceTimestamp" : str(record.SourceTimestamp),
            "SourcePicosec" : str(record.SourcePicoseconds),
            "ServerTimestamp" : str(record.ServerTimestamp),
            "ServerPicosec" : str(record.ServerPicoseconds)
        })
    print(json.dumps(parsed_records, indent=2))

def get_opcua_node(g, signal):
    sparql_query = """
    PREFIX core: <https://purl.org/signal-description-ontology/core#>
    PREFIX opcua: <https://purl.org/signal-description-ontology/opcua#>
    PREFIX : <https://purl.org/signal-description-ontology/abox#>

    SELECT ?ns_uri ?id ?url ?stype
    WHERE {{
        {_signal} core:hasHistAccess ?access.
        {_signal} core:hasSignalType ?stype.
        ?access rdf:type opcua:OpcUaAccess.
        ?access opcua:hasNamespaceURI ?ns_uri.
        ?access opcua:hasIdentifier ?id.
        ?access opcua:hasOpcUaConnectionInfo ?connection.
        ?connection core:hasURL ?url.
    }}
    """.format(_signal=signal)
    res = list(g.query(sparql_query))
    return res[0]

##################################################################
# Main function
##################################################################

async def main():
    g = Graph()
    g.parse("onto_v01.ttl", format="turtle")
    g.parse("abox.ttl", format="turtle")

    if sys.argv[1] == "discover":
        await influxdb_discovery(g)
        await opcua_discovery(g)
    elif sys.argv[1] == "interpolation":
        await plot_signals(g)

#################################################################
# Test: Discover InfluxDB points
#################################################################

async def influxdb_discovery(g):
    print("""
-------------------------------------------------------------
  InfluxDB
-------------------------------------------------------------""")

    influxdb_url = "http://127.0.0.1:8086"
    influxdb_connection = get_influxdb_connection(g, influxdb_url)

    token = get_token(g, influxdb_connection)
    org = get_organization(g, influxdb_connection)
    bucket = get_bucket(g, influxdb_connection)

    measurement = "machine_data"

    signal_field_pairs = get_signal_field_pairs(g, influxdb_connection, measurement)

    async with InfluxDBClientAsync(url=influxdb_url, token=token, org=org) as client:

        for signal, field in signal_field_pairs:
            tags = get_influxdb_tags(g, f"<{str(signal)}>")

            flux_query = create_flux_query(bucket, measurement, tags, field, 2)
            
            influxdb_result = await client.query_api().query(flux_query)
            print(str(signal))
            print(influxdb_result.to_json())

#################################################################
# Test: Discover OPC UA nodes
#################################################################

async def opcua_discovery(g):
    print("""
-------------------------------------------------------------
  OPC UA
-------------------------------------------------------------""")

    opcua_url = "opc.tcp://127.0.0.1:4840/server1/"
    opcua_connection = get_opcua_connection(g, opcua_url)
    username = "admin"
    password = get_password(g, opcua_connection, username)
    
    client = Client(url=opcua_url)
    client.set_user(username)
    client.set_password(password)

    ns_cache = {}

    start_time = datetime.now() - timedelta(minutes=1)
    end_time = datetime.now()

    # ReadRawModifiedDetails: https://reference.opcfoundation.org/Core/Part11/v105/docs/6.5.3
    hist_read_details = ua.ReadRawModifiedDetails()
    hist_read_details.IsReadModified = False
    hist_read_details.StartTime = start_time
    hist_read_details.EndTime = end_time
    hist_read_details.NumValuesPerNode = 2
    hist_read_details.ReturnBounds = True

    # HistoryRead: https://reference.opcfoundation.org/Core/Part4/v105/docs/5.11.3
    hist_parameters = ua.HistoryReadParameters()
    hist_parameters.HistoryReadDetails = hist_read_details
    hist_parameters.TimestampsToReturn = ua.TimestampsToReturn.Both
    hist_parameters.ReleaseContinuationPoints = False

    async with client:

        hist_nodes = get_hist_nodes(g, opcua_connection)

        for node in hist_nodes:
            uri = str(node["uri"])

            idx = ns_cache.get(uri)
            if idx is None:
                idx = await client.get_namespace_index(uri)
                ns_cache[uri] = idx
            
            if node["id"].datatype == XSD.integer:
                id_type = "i"
            else:
                id_type = "s"
            id_str = f"ns={idx};{id_type}={node["id"]}"

            print(f"https://purl.org/signal-description-ontology/abox#{node["asset_label"]}_{node["signal_label"]}: {id_str}")

            hrv = ua.HistoryReadValueId()
            hrv.NodeId = ua.NodeId(node["id"], idx)

            # all nodes in this list are read at once
            hist_parameters.NodesToRead = [hrv]

            opcua_result = await client.uaclient.history_read(hist_parameters)
            
            # HistoryData: https://reference.opcfoundation.org/Core/Part11/v105/docs/6.6
            # DataValues: https://reference.opcfoundation.org/Core/Part4/v105/docs/7.11
            print_datavalues(opcua_result[0].HistoryData.DataValues)

#################################################################
# Test: From observation to signal
#################################################################

def get_interpolation(signal_type):
    if str(signal_type) == "LinearInterpolation":
        return "linear"
    elif str(signal_type) == "HoldObservations":
        return "previous"
    else:
        return None

async def plot_signals(g):
    
    #################################################################
    # OPC UA query: Query :Press_MaterialTemperature
    #################################################################

    # get OPC UA node information from ontology
    node = get_opcua_node(g, ":Press_MaterialTemperature")

    opcua_connection = get_opcua_connection(g, node["url"])
    username = "admin"
    password = get_password(g, opcua_connection, username)

    opcua_client = Client(url=node["url"])
    opcua_client.set_user(username)
    opcua_client.set_password(password)

    start_time = datetime.now() - timedelta(seconds=8)
    end_time = datetime.now()

    # ReadRawModifiedDetails: https://reference.opcfoundation.org/Core/Part11/v105/docs/6.5.3
    hist_read_details = ua.ReadRawModifiedDetails()
    hist_read_details.IsReadModified = False
    hist_read_details.StartTime = start_time
    hist_read_details.EndTime = end_time
    hist_read_details.NumValuesPerNode = 8
    hist_read_details.ReturnBounds = True

    # HistoryRead: https://reference.opcfoundation.org/Core/Part4/v105/docs/5.11.3
    hist_parameters = ua.HistoryReadParameters()
    hist_parameters.HistoryReadDetails = hist_read_details
    hist_parameters.TimestampsToReturn = ua.TimestampsToReturn.Both
    hist_parameters.ReleaseContinuationPoints = False

    t_press_temp, x_press_temp = [], []
    async with opcua_client:

        idx = await opcua_client.get_namespace_index(str(node["ns_uri"]))

        hrv = ua.HistoryReadValueId()
        hrv.NodeId = ua.NodeId(node["id"], idx)

        hist_parameters.NodesToRead = [hrv]
        opcua_results = await opcua_client.uaclient.history_read(hist_parameters)

        for data_value in opcua_results[0].HistoryData.DataValues:
            x_press_temp.append(data_value.Value.Value)
            t_press_temp.append(data_value.ServerTimestamp)

    #################################################################
    # InfluxDB query: Query :Extruder_Temperature
    #################################################################

    signal = ":Extruder_Temperature"
    point = get_influx_point(g, signal)
    tags = get_influxdb_tags(g, signal)
    # extruder_tags = [('Factory', 'VIE'), ('Room', 'Room1'), ('Line', 'Line1'), ('Machine', 'Extruder')]
    
    influxdb_connection = get_influxdb_connection(g, point["url"])
    token = get_token(g, influxdb_connection)

    t_extruder_temp, x_extruder_temp = [], []

    async with InfluxDBClientAsync(url=point["url"], token=token, org=point["org"]) as influx_client:

        flux_query = create_flux_query(point["bucket"], point["measurement"], tags, 'Temperature')

        influxdb_result = await influx_client.query_api().query(flux_query)
        for table in influxdb_result:
            for record in table.records:
                t_extruder_temp.append(record.get_time())
                x_extruder_temp.append(record.get_value())
    
    #################################################################
    # Plot temperature
    #################################################################

    # prepare grid
    t_press = np.array([ts.timestamp() for ts in t_press_temp])
    x_press = np.array(x_press_temp)

    t_extr = np.array([ts.timestamp() for ts in t_extruder_temp])
    x_extr = np.array(x_extruder_temp)
    # grid borders
    t_min = max(t_press.min(), t_extr.min())
    t_max = min(t_press.max(), t_extr.max())

    t_common = np.linspace(t_min, t_max, 500)
    # convert timestamp to date in HH:MM:SS
    t_common_dt = [datetime.fromtimestamp(t) for t in t_common]

    # ToDo: query interpolation
    f_press = interp1d(t_press, x_press, kind=get_interpolation(node["stype"]))
    x_press_interp = f_press(t_common)
    f_extr = interp1d(t_extr, x_extr, kind=get_interpolation(point["stype"]))
    x_extr_interp = f_extr(t_common)

    x_avg = (x_press_interp + x_extr_interp) / 2

    
    # plot of discrete observations
    plt.figure(figsize=(6, 3.5))
    markerline, stemlines, baseline = plt.stem(t_press_temp, x_press_temp, markerfmt="x", linefmt="C0-.", basefmt=" ")
    plt.setp(stemlines, alpha=0.5)
    markerline, stemlines, baseline = plt.stem(t_extruder_temp, x_extruder_temp, markerfmt="x", linefmt="C1-.", basefmt=" ")
    plt.setp(stemlines, alpha=0.5)

    plt.plot(t_press_temp, x_press_temp, "C0:", label="Press")
    plt.step(t_extruder_temp, x_extruder_temp, "C1:", where="post", label="Extruder")

    plt.ylim(bottom=40, top=120)
    plt.xlim(min(t_common_dt), max(t_common_dt))
    plt.legend(loc="lower right")
    plt.xlabel("Time")
    plt.ylabel("Temperature [°C]")
    plt.grid(True)
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.savefig("discrete_plot.pdf", format="pdf", bbox_inches="tight")
    plt.close()

    # plot of continuos signals
    plt.figure(figsize=(6, 3.5))
    plt.plot(t_common_dt, x_press_interp, "C0-", label="Press")
    plt.step(t_common_dt, x_extr_interp, "C1-", where="post", label="Extruder")
    plt.plot(t_common_dt, x_avg, "C2--", label="Avg.")

    plt.ylim(bottom=40, top=120)
    plt.xlim(min(t_common_dt), max(t_common_dt))
    plt.legend(loc="lower right")
    plt.xlabel("Time")
    plt.ylabel("Temperature [°C]")
    plt.grid(True)
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.savefig("continuous_plot.pdf", format="pdf", bbox_inches="tight")
    plt.close()

if __name__ == "__main__":
    asyncio.run(main())