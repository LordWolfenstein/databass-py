'''
DataBass but for SQLite
'''
import sqlite3
from tabulate import tabulate
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
            print("insupd list")
            return [self.insupd(table, d) for d in data]
        if self._exists(table, data):
            prim = self.primary_keys(table)
            deletedata = {key: data[key] for key in data if key in prim}
            self.delete(table, deletedata)
        return self.insert(table, data)

    def create_table(self, code: str) -> bool:
        '''TODO: put in a check do no table or column got the name "PRIMARY KEY"'''
        print(code)
        return False

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
    '''Pretty prints the list of dictionaries returned by databass.run()
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
