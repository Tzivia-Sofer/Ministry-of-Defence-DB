from dataclasses_json import dataclass_json
import db_api
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
import json


@dataclass_json
@dataclass
class Edge:
    weight: int


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any


# -----------------AUXILIARY FUNCTIONS---------------
def read_from_file(file_name):
    with open(os.path.join("db_files", file_name), 'r') as file:
        data = json.load(file)
    return data


def write_to_file(data, file_name):
    with open(os.path.join("db_files", file_name), 'w') as file:
        json.dump(data, file, default=str)


def remove_connections(self, key, connections: Dict[str, Edge], out: bool):
    edge_to_remove = "edges in" if out else "edges out"
    for vertex_key in connections.keys():
        file_name = str(vertex_key) + ".json"
        data = read_from_file(file_name)
        with open(os.path.join("db_files", file_name), 'w') as file:
            del data[edge_to_remove][key]
            json.dump(data, file, default=str)


def update_connections(key_field_name: str, vertex_to_insert: str, vertex_list: list, record: Dict[str, Edge],
                       out: bool):
    field_to_update = "edges in" if out else "edges out"
    empty_field = "edges out" if out else "edges in"
    for key, edge in record.items():
        file_name = str(key) + '.json'
        if os.path.exists(os.path.join("db_files", file_name)):
            data = read_from_file(file_name)
            data[field_to_update][vertex_to_insert] = edge
            write_to_file(data, file_name)
        else:
            new_vertex = {key_field_name: key, field_to_update: {vertex_to_insert: edge}, empty_field: {}}
            write_to_file(new_vertex, file_name)
        vertex_list.append(key) if key not in vertex_list else []


# ------------------------------------------------------
def get_out_edges(vertex):
    data = read_from_file(vertex+".json")
    return list(data["edges out"].keys())


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    vertex_list = []

    def find_matching(self, criteria: List[SelectionCriteria]) -> list:
        fitting_vertex = []
        for vertex in self.vertex_list:
            match = True
            data = self.get_record(vertex)
            for selection in criteria:
                if selection.field_name == "edges in":
                    if "edges in" in data:
                        if not eval(str(len(data["edges in"])) + selection.operator + str(selection.value)):
                            match = False
                elif selection.field_name == "edges out":
                    if "edges out" in data:
                        if not eval(str(len(data["edges out"])) + selection.operator + str(selection.value)):
                            match = False
                else:
                    if selection.field_name in data:
                        if selection.operator == "<":
                            if not data[selection.field_name] < selection.value:
                                match = False
                        elif selection.operator == ">":
                            if not data[selection.field_name] > selection.value:
                                match = False
                        elif selection.operator == "==":
                            if not data[selection.field_name] == selection.value:
                                match = False
            fitting_vertex.append(vertex) if match else []
        return fitting_vertex

    def insert_record(self, record: Dict[str, Any]) -> None:
        file_name = str(record[self.key_field_name]) + '.json'
        exists = False
        if os.path.exists(os.path.join("db_files", file_name)):
            data = read_from_file(file_name)
            exists = True
            for attribute, value in record.items():
                if attribute == "edges in":
                    if attribute in data:
                        data[attribute].update(value)
                    else:
                        data[attribute] = value
                elif attribute == "edges out":
                    if attribute in data:
                        data[attribute].update(value)
                    else:
                        data[attribute] = record[value]
                else:
                    data[attribute] = value
            if "edges in" not in data:
                data["edges in"] = {}
            if "edges out" not in data:
                data["edges out"] = {}
            write_to_file(data, file_name)
        if "edges in" in record:
            update_connections(self.key_field_name, record[self.key_field_name], self.vertex_list, record["edges in"],
                               False)
        else:
            record["edges in"] = {}
        if "edges out" in record:
            update_connections(self.key_field_name, record[self.key_field_name], self.vertex_list, record["edges out"],
                               True)
        else:
            record["edges out"] = {}
        if not exists:
            write_to_file(record, file_name)
        self.vertex_list.append(record[self.key_field_name]) if record[
                                                                    self.key_field_name] not in self.vertex_list else []

    def count(self) -> int:
        return len(self.vertex_list)

    def delete_record(self, key: Any) -> None:
        file_name = str(key) + ".json"
        if os.path.exists(os.path.join("db_files", file_name)):
            data = read_from_file(file_name)
            if "edges in" in data:
                remove_connections(self, key, data["edges in"], False)
            if "edges out" in data:
                remove_connections(self, key, data["edges out"], True)
            if os.path.exists(os.path.join("db_files", file_name)):
                os.remove(os.path.join("db_files", file_name))
            self.vertex_list.remove(key)
        else:
            raise ValueError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        vertex_to_delete = self.find_matching(criteria)
        for vertex in vertex_to_delete:
            self.delete_record(vertex)

    def get_record(self, key: Any) -> Dict[str, Any]:
        file_name = str(key) + '.json'
        data = read_from_file(file_name)
        return data

    def update_record(self, key: str, values: Dict[str, Any]) -> None:
        file_name = str(key) + '.json'
        data = read_from_file(file_name)
        for attribute, value in values.items():
            if attribute == "edges in":
                self.insert_record({self.key_field_name: key, attribute: value})
                data["edges in"].update(value)
            if attribute == "edges out":
                self.insert_record({self.key_field_name: key, attribute: value})
                data["edges out"].update(value)
            else:
                data[attribute] = value
        write_to_file(data, file_name)

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        matching_vertexes = self.find_matching(criteria)
        vertexes_to_return = []
        for vertex in matching_vertexes:
            vertexes_to_return.append(self.get_record(vertex))
        return vertexes_to_return

    def find_path(self, start, end, path=None):
        if path is None:
            path = []

        path = path + [start]

        if start == end:
            return path

        if start not in self.vertex_list:
            return None

        for connection in get_out_edges(start):
            if connection not in path:
                new_path = self.find_path(connection, end, path)
                if new_path:
                    return new_path

        return None

    def find_all_paths(self, start, end, path=None):
        if path is None:
            path = []
        path = path + [start]
        if start == end:
            return [path]
        if start not in self.vertex_list:
            return []
        paths = []
        for node in get_out_edges(start):
            if node not in path:
                new_paths = self.find_all_paths(node, end, path)
                for new_path in new_paths:
                    paths.append(new_path)
        return paths

    def find_shortest_path(self, start, end, path=None):
        if path is None:
            path = []
        path = path + [start]
        if start == end:
            return path
        if start not in self.vertex_list:
            return None
        shortest = None
        for node in get_out_edges(start):
            if node not in path:
                new_path = self.find_shortest_path(node, end, path)
                if new_path:
                    if not shortest or len(new_path) < len(shortest):
                        shortest = new_path
        return shortest




@dataclass_json
@dataclass
class DataBase(db_api.DataBase):

    def __init__(self):
        self.tables_list = {}

        files = os.listdir(db_api.DB_ROOT)
        files = list(filter(lambda f: f.endswith('.json'), files))

        db = DBTable("Students", [db_api.DBField("ID", str),
                                  db_api.DBField("edges in", Dict[str, Edge]),
                                  db_api.DBField("edges out", Dict[str, Edge])], "ID")

        for file in files:
            db.vertex_list.append(file[:-5])

        self.tables_list["Students"] = db

    def create_table(self, table_name: str, fields: List[db_api.DBField], key_field_name: str) -> DBTable:
        if key_field_name not in list(map(lambda x: x.name, fields)):
            raise ValueError
        table = DBTable(table_name, fields, key_field_name)
        self.tables_list[table_name] = table
        return table

    def num_tables(self) -> int:
        return len(self.tables_list)

    def get_table(self, table_name: str) -> DBTable:
        return self.tables_list[table_name]

    def delete_table(self, table_name: str) -> None:
        del self.tables_list[table_name]

    def get_tables_names(self) -> List[Any]:
        return list(self.tables_list.keys())

    def query_multiple_tables(self, tables: List[str], fields_and_values_list: List[List[SelectionCriteria]], fields_to_join_by: List[str]) -> List[Dict[str, Any]]:
        raise NotImplementedError


if __name__ == '__main__':
    vertex = []

    db = DBTable("my table", [db_api.DBField("name", str), db_api.DBField("population", int),
                              db_api.DBField("edges in", Dict[str, Edge]),
                              db_api.DBField("edges out", Dict[str, Edge])], "name")

    Tel_Aviv = {"name": "Tel Aviv", "population": 100, "edges in": {"Yafo": 16},
                "edges out": {"Eilat": 80, "Yahud": 30}}

    Yahud = {"name": "Yahud", "edges out": {"Ganot": 13, "Yafo": 8}}

    db.insert_record(Yahud)
    db.insert_record(Tel_Aviv)
    db.insert_record({"name": "Ganot", "edges out": {"Yafo": 13}})
#     dbs = DataBase()
    print(db.find_shortest_path("Tel Aviv", "Yafo"))
