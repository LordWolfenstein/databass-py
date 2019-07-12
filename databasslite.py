'''Databass is a interface meant to simplify database transactions in Python
by letting you use dictionaries and list of dictionaries for your transactions
instead of writing pure SQL-code. Thus easily letting you generate and read
JSON-feeds. Preferably over HTTPS.
The syntax for doing operations and generating feeds is identical. And the
result from reading the feed is identical to as if the operation were done
locally.
(Only tested with Python 3. (Automatic JOIN with dictionaries is on the TODO
list. But you can still write your own SQL and get the list of dictionaries
from joined SELECTS.))
NAME
Databass is a punmanteau from "data" and "bass" because bass sound like base.
And you can feed the bass. Bass feed is much funnier than Atom feed.
Copyright (C) 2018 Lord Wolfenstein
https://github.com/LordWolfenstein/databass
MIT License
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''
import re
import sqlite3
from lib.tabulate import tabulate
from typing import Union

class DataBassLite:
    '''DataBass but for SQLite'''

    @staticmethod
    def _dict_factory(cursor, row):
        '''Taken from the docs.'''
        dic = {}
        for idx, col in enumerate(cursor.description):
            dic[col[0]] = row[idx]
        return dic

    def __init__(self, file: str):
        self.sql = sqlite3.connect(file)
        self.sql.row_factory = self._dict_factory

    def tables(self) -> list:
        '''Returns a list of tables in the database'''
        tables = self.run("SELECT `name` FROM `sqlite_master` WHERE type='table';")
        ret = [table["name"] for table in tables]
        return ret

    def columns(self, table: str) -> Union[list, None]:
        '''Returns the columns in the table.'''
        if table not in self.tables():
            print("ERROR: table", table, "not in database")
            return None
        query = """--begin-sql
        SELECT * FROM `{}`;
        """.format(table)
        cur = self.sql.cursor()
        cur.execute(query)
        columns = cur.description
        ret = [column[0] for column in columns]
        return ret

    def create(self, tableconfig: dict) -> list:
        '''Creates a table from a dictionary
           TODO: Fix to the datatypes in the dictionary to match the other databass.
                 The end goal it to make them 100% compatible. (But this will have to do for now!)
           tableconfigs = {
                "tablename1" :
                [
                    {
                        "Field": "id",
                        "Type": "INT",
                        "Key": "PRI"
                    },
                    {
                        "Field": "text",
                        "Type": "text",
                        "Key": ""
                    }
                ],
                "tablename2" :
                [
                    {
                        "Field": "id",
                        "Type": "INT",
                        "Key": "PRI"
                    },
                    {
                        "Field": "time",
                        "Type": "double",
                        "Key": ""
                    }
                ]
            }
        '''
        ret = []
        for table in tableconfig:
            pattern = re.compile("[a-zA-Z0-9_ ]*")
            if table in self.tables():
                ret.append("Table {} already exists".format(table))
            elif not pattern.fullmatch(table):
                ret.append("Table {} contains illigal characters.".format(table))
            else:
                columns = []
                keys = []
                for column in tableconfig[table]:
                    if not pattern.fullmatch(column["Field"]):
                        ret.append("Column {} contain illigal characters.".format(column["Field"]))
                        continue
                    columns.append("{} {}".format(column["Field"], column["Type"]))
                    if column["Key"].upper() == "PRI":
                        keys.append(column["Field"])
                columns = ",\n".join(columns)
                keys = ", ".join(keys)
                query = """--begin-sql
                CREATE TABLE {}
                ({},
                PRIMARY KEY({}));
                """.format(table, columns, keys)
                # print(query)
                ret.append(self.run(query))
        return ret

    def primary_keys(self, table):
        '''Returns the primary keys in the table.'''
        if table not in self.tables():
            print("ERROR: table", table, "not in database")
            return None
        everything = self.run("SELECT * FROM `sqlite_master` WHERE type='table' AND name=?;",
                              (table,))
        code = everything[0]["sql"]
        code = code.replace("\n", " ")
        while "  " in code:
            code = code.replace("  ", " ")
        code = code.split("PRIMARY KEY")[1]
        code = code.strip("(").strip(")").strip()
        code = code.split(",")
        primarykeys = [key.strip() for key in code]
        return primarykeys

    def distinct(self, table: str, where: dict = {}, wherenot: dict = {},
                 columns: dict = ["*"]) -> Union[list, None]:
        '''Sistinct select statement'''
        return self.select(table, where, wherenot, columns, True)

    def select(self, table: str, where: dict = {}, wherenot: dict = {},
               columns: dict = ["*"], distinct: bool = False) -> Union[list, None]:
        '''Select statement'''
        if table not in self.tables():
            print("ERROR: table", table, "not in database")
            return None
        tablecolumns = self.columns(table)
        for column in where.keys():
            if column not in tablecolumns:
                print("ERROR: column", column, "not in table", table)
                return None
        for column in wherenot.keys():
            if column not in tablecolumns:
                print("ERROR: column", column, "not in table", table)
                return None
        if columns != ["*"]:
            if isinstance(columns, str):
                columns = [columns]
            for column in columns:
                if column not in tablecolumns:
                    print("ERROR: column", column, "not in table", table)
                    return None
        values = ()
        whereclause = ""
        if where != {} or wherenot != {}:
            whereclause += "WHERE "
        if where != {}:
            keys = list(where.keys())
            wherecolumns = ["`{}`=?".format(key) for key in keys]
            whereclause += " AND ".join(wherecolumns)
            values += tuple([where[key] for key in keys])
        if wherenot != {}:
            keys = list(wherenot.keys())
            wherenotcolumns = ["`{}`=?".format(key) for key in keys]
            whereclause += " AND ".join(wherenotcolumns)
            values += tuple([wherenot[key] for key in keys])
        if distinct:
            query = """--begin-sql
            SELECT DISTINCT {}
            FROM `{}`
            {};
            """.format(", ".join(columns), table, whereclause)
        else:
            query = """--begin-sql
            SELECT {}
            FROM `{}`
            {};
            """.format(", ".join(columns), table, whereclause)
        # print("query =", query)
        # print("values =", values)
        return self.run(query, values)

    def insert(self, table: str, data: Union[dict, tuple]) -> Union[None, str]:
        '''Inserts data in to database'''
        if table not in self.tables():
            print("ERROR: table", table, "not in database")
            return False
        if isinstance(data, dict):
            data = [data]
        tablecolumns = self.columns(table)
        for row in data:
            for column in row.keys():
                if column not in tablecolumns:
                    print("ERROR: column", column, "not in table", table)
                    return False
        columns = list(data[0].keys())
        questionmarks = ", ".join(["?" for i in columns])
        query = """--begin-sql
        INSERT INTO {} ({}) VALUES ({});
        """.format(table, ", ".join(columns), questionmarks)
        values = []
        for row in data:
            values.append(tuple(row[key] for key in columns))
        # print(query, values)
        return self.run(query, values)

    def _exists(self, table: str, data: dict) -> bool:
        '''Checks if the entry exists in the database. Only checks for primary keys'''
        prim = self.primary_keys(table)
        columns = {key: data[key] for key in data if key in prim}
        return self.select(table, columns) != []

    def delete(self, table: str, where: dict) -> None:
        '''Deletes a row.'''
        if table not in self.tables():
            print("ERROR: table", table, "not in database")
            return False
        tablecolumns = self.columns(table)
        for column in where.keys():
            if column not in tablecolumns:
                print("ERROR: column", column, "not in table", table)
                return False
        keys = list(where.keys())
        wherecolumns = ["`{}`=?".format(key) for key in keys]
        values = tuple([where[key] for key in keys])
        query = """--begin-sql
        DELETE FROM {}
        WHERE {};
        """.format(table, " AND ".join(wherecolumns))
        return self.run(query, values)

    def insupd(self, table: str, data: Union[dict, list]) -> Union[list, str]:
        '''Inserts if not existing, updates on existing'''
        if isinstance(data, list):
            return [self.insupd(table, d) for d in data]
        if self._exists(table, data):
            prim = self.primary_keys(table)
            deletedata = {key: data[key] for key in data if key in prim}
            self.delete(table, deletedata)
        return self.insert(table, data)

    def drop(self, table: str) -> None:
        '''Drops a table.'''
        if table not in self.tables():
            return
        query = """--begin-sql
        DROP TABLE {};
        """.format(table)
        self.run(query)

    def run(self, query: str, values: Union[tuple, list, None] = None) -> Union[list, None]:
        '''_'''
        cur = self.sql.cursor()
        cur.execute('pragma encoding=utf8')
        # print("query =", query)
        # print("values =", values)
        if values is None:
            cur.execute(query)
        elif isinstance(values, tuple):
            cur.execute(query, values)
        elif isinstance(values, list):
            cur.executemany(query, values)
        else:
            print("ERROR: strange values", values)
            return None
        result = cur.fetchall()
        self.sql.commit()
        return result

def printrows(rows: list, grid: str = "presto") -> None:
    '''Pretty prints the list of dictionaries returned by DataBassLite.run()
    data: A list of dictionaries
    '''
    if isinstance(rows, list):
        if rows != []:
            print(tabulate([i.values() for i in rows], rows[0].keys(), grid))
        else:
            print(rows)
    else:
        print(rows)

def printframed(string: str) -> None:
    '''prints a word in a nice frame'''
    length = len(string)
    print("+-" + "-"*length + "-+")
    print("| " +     string + " |")
    print("+-" + "-"*length + "-+")
