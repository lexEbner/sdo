import asyncio
import numpy as np
from datetime import datetime

from asyncua import Server, ua
from asyncua.server.users import UserRole, User
from asyncua.common.methods import uamethod
from asyncua.server.history_sql import HistorySQLite

users_db = {
    "admin": "PWD@server1",
    "al": "12345678"
}


def make_prob_generator(lower, upper, weight, counter_max):
    value = np.random.randint(lower, upper)
    counter = 0
    def next_prob_value():
        nonlocal value, counter
        if counter != 0:
            if counter < counter_max:
                value += np.random.randint(1, 10) * weight
            else:
                value = np.random.randint(lower, upper)
                counter = 0
        counter += 1
        return round(value, 3)
    return next_prob_value

def make_lin_generator(init_value, increment, counter_max):
    value = init_value
    counter = 0
    def next_lin_value():
        nonlocal value, counter
        if counter != 0:
            if counter < counter_max:
                value += increment
            else:
                value = init_value
                counter = 0
        counter += 1
        return round(value, 3)
    return next_lin_value

def make_sin_generator(amplitude, omega, phase, offset):
    counter = 0
    def next_sin_value():
        nonlocal counter
        value = offset + amplitude * np.sin(omega * counter + phase)
        counter += 1
        return round(value, 3)
    return next_sin_value


def make_rand_int_generator(lower, upper):
    def rand_int():
        return np.random.randint(lower, upper)
    return rand_int

class UserManager:
    def get_user(self, iserver, username=None, password=None, certificate=None):
        if username in users_db and password == users_db[username]:
            if username == "admin":
                return User(role=UserRole.Admin)
            return User(role=UserRole.User)
        return None

async def main():
    server = Server(user_manager=UserManager())
    history = HistorySQLite("history_db.sql")
    server.iserver.history_manager.set_storage(history)

    await server.init()
    server.set_endpoint("opc.tcp://127.0.0.1:4840/server1/")
    
    ns_uri = "http://example.org/opcua/"
    # namespace index
    idx = await server.register_namespace(ns_uri)

    generators = {
        "Line2": {
            "UnitsPerBatch": make_rand_int_generator(100, 150)
        },
        "Extruder": {
            "Temperature": make_sin_generator(45, 2*np.pi / 5.1234, 1, 250),
            "Throughput": make_prob_generator(400, 405, 0.23, 11)
        },
        "Press": {
            "MaterialTemperature": make_rand_int_generator(50, 120),
            "Pressure": make_lin_generator(110, 7.3, 15)
        },
        "Cutter": {
            "BladeSpeed": make_prob_generator(3200, 3400, -2.9, 7),
            "Pressure": make_lin_generator(120, 11.7, 11)
        },
        "Belt": {
            "Speed": make_prob_generator(18, 20, 0.1, 4)
        }
    }

    ## type definitions

    asset_obj_t = await server.nodes.base_object_type.add_object_type(idx, "AssetObjectType")
    factory_obj_t = await asset_obj_t.add_object_type(idx, "FactoryObjectType")
    room_obj_t = await asset_obj_t.add_object_type(idx, "RoomObjectType")
    line_obj_t = await asset_obj_t.add_object_type(idx, "LineObjectType")
    machine_obj_t = await asset_obj_t.add_object_type(idx, "MachineObjectType")

    ## object instances

    factory_vie = await server.nodes.objects.add_object(
        ua.NodeId(100000, idx), "FactoryVIE", factory_obj_t
    )
    room1 = await factory_vie.add_object(
        ua.NodeId(110000, idx), "Room1", room_obj_t
    )
    line1 = await room1.add_object(
        ua.NodeId(111000, idx), "Line1", line_obj_t
    )
    line2 = await room1.add_object(
        ua.NodeId(112000, idx), "Line2", line_obj_t
    )
    extruder = await line1.add_object(
        ua.NodeId(111100, idx), "Extruder", machine_obj_t
    )
    press = await line1.add_object(
        ua.NodeId(111200, idx), "Press", machine_obj_t
    )
    cutter = await line1.add_object(
        ua.NodeId(111300, idx), "Cutter", machine_obj_t
    )
    belt = await line2.add_object(
        ua.NodeId(112100, idx), "Belt", machine_obj_t
    )
    
    ## variable instances

    # line 2: units per batch (-)
    line2_units = await line2.add_variable(
        ua.NodeId(112001, idx), "UnitsPerBatch", generators["Line2"]["UnitsPerBatch"](), ua.VariantType.Int32
    )
    # extruder: temperature (C)
    extruder_temp = await extruder.add_variable(
        ua.NodeId(111101, idx), "Temperature", generators["Extruder"]["Temperature"](), ua.VariantType.Float
    )
    # extruder: throughput (kg/h)
    extruder_throughput = await extruder.add_variable(
        ua.NodeId(111102, idx), "Throughput", generators["Extruder"]["Throughput"](), ua.VariantType.Float
    )
    # press: material temperature (C)
    press_temp = await press.add_variable(
        ua.NodeId(111201, idx), "MaterialTemperature", generators["Press"]["MaterialTemperature"](), ua.VariantType.Float
    )
    # press: pressure (bar)
    press_pressure = await press.add_variable(
        ua.NodeId(111202, idx), "Pressure", generators["Press"]["Pressure"](), ua.VariantType.Float
    )
    # cutter: blade speed (rpm)
    cutter_speed = await cutter.add_variable(
        ua.NodeId(111301, idx), "BladeSpeed", generators["Cutter"]["BladeSpeed"](), ua.VariantType.Float
    )
    # cutter: blade pressure (N)
    cutter_pressure = await cutter.add_variable(
        ua.NodeId(111302, idx), "Pressure", generators["Cutter"]["Pressure"](), ua.VariantType.Float
    )
    # belt: speed (m/min)
    belt_speed = await belt.add_variable(
        ua.NodeId(112101, idx), "Speed", generators["Belt"]["Speed"](), ua.VariantType.Float
    )

    await server.historize_node_data_change(
        [line2_units, press_temp, press_pressure, cutter_speed, cutter_pressure], period=None, count=100
    )

    nodes = {
        "Line2": {
            "UnitsPerBatch": line2_units
        },
        "Extruder": {
            "Temperature": extruder_temp,
            "Throughput": extruder_throughput
        },
        "Press": {
            "MaterialTemperature": press_temp,
            "Pressure": press_pressure
        },
        "Cutter": {
            "BladeSpeed": cutter_speed,
            "Pressure": cutter_pressure
        },
        "Belt": {
            "Speed": belt_speed
        }
    }

    async with server:
        print("Server is running...")
        while True:
            for producer, variables in nodes.items():
                for variable, node in variables.items():

                    if (producer == "Line2" and variable == "UnitsPerBatch"):
                        await node.set_value(generators[producer][variable](), ua.VariantType.Int32)
                    else:
                        await node.set_value(generators[producer][variable](), ua.VariantType.Float)
            await asyncio.sleep(1 + np.random.uniform(-0.1, 0.1))

if __name__ == "__main__":
    asyncio.run(main())