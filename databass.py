'''Databass is a interface meant to simplyfy database transactions in Python 
by letting you use dictionaries and list of dictionaries for your transactions
instead of writing pure SQL-code. Thus easily letting you generate and read 
JSON-feeds. Preferbly over HTTPS.

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
import mysql.connector as MariaDB
from tabulate import tabulate
import json

class databass:
    '''Class that simplifies database connections.'''
    __version__ = 0.2
    
    def __init__(self, config):
        '''Config format:
        config = {'user'     : 'root',
                  'password' : 'pass',
                  'host'     : '1.2.3.4',
                  'port'     : '3306',
                  'database' : 'test'}'''
        self._bass = MariaDB.connect(**config)
        self._cursor  = self._bass.cursor(dictionary=True)

        # Feed eating functions
        self._feedeaters ={}
        self._feedeaters["create"]      = self.EatCreate
        self._feedeaters["alter table"] = self.EatAlterTable
        self._feedeaters["drop"]        = self.EatDrop
        self._feedeaters["insert"]      = self.EatInsert
        self._feedeaters["update"]      = self.EatUpdate
        self._feedeaters["delete"]      = self.EatDelete


    def run(self, sql):
        '''Runs a query.
           Returns a list of dictionaries on successfull SELECT.
        '''
        #print("run:", sql)
        try:
            self._cursor.execute(sql)
        except MariaDB.Error as err:
            return "Database Error: " + str(err)
        if self._cursor.description:
            ret=[]
            for row in self._cursor:
                ret.append(row)
            self._bass.commit()
            return ret
        else:
            self._bass.commit()
            return True

    def count(self, table):
        '''Returns the number of rows in a given table.'''
        return self.run("SELECT count(*) FROM `{}`".format(table))[0]["count(*)"]

    def name(self):
        '''Returns the name of the currently selected database.'''
        return self.run("SELECT DATABASE()")[0]["DATABASE()"]

    def tables(self):
        '''Returns a list of tables in the database.'''
        result = self.run("SHOW tables")
        tables = []
        name = self.name()
        for table in result:
            tables.append(table["Tables_in_" + name])
        return tables

    def info(self, table):
        '''Returns detailed table info in dictionary form'''
        return self.run("DESCRIBE `{}`".format(table))

    def code(self, table):
        '''Returns the code used to create the table'''
        return self.run("SHOW CREATE TABLE `{}`".format(table))[0]["Create Table"]

    def drop(self, table):
        '''Drops the table'''
        return self.run("DROP TABLE `{}`".format(table))

    def create(self, tableconfigs):
        '''Creates a table according to the given condig.
        This used the same syntax that MariaDB used when you DESCRIBE a table
        tableconfigs is a dictionary of the format
        tableconfigs = {
            "tablename1" :
            [
                {
                    "Field": "id",
                    "Type": "int(11)",
                    "Null": "NO",
                    "Key": "PRI",
                    "Default": "None",
                    "Extra": "auto_increment"
                },
                {
                    "Field": "text",
                    "Type": "text",
                    "Null": "YES",
                    "Key": "",
                    "Default": "'Nothing!'",
                    "Extra": ""
                }
            ],
            "tablename2" :
            [
                {
                    "Field": "id",
                    "Type": "int(11)",
                    "Null": "NO",
                    "Key": "PRI",
                    "Default": "None",
                    "Extra": "auto_increment"
                },
                {
                    "Field": "tid",
                    "Type": "double",
                    "Null": "YES",
                    "Key": "",
                    "Default": "None",
                    "Extra": ""
                }
            ]
        }
        '''
        ret = ""
        for t in tableconfigs:
            # print(tableconfigs[t])
            sql = "CREATE TABLE `{}` (".format(t)
            for column in tableconfigs[t]:
                sql += "`{}` {} {} {} {}, ".format(column["Field"], 
                column["Type"], 
                ("NOT NULL" if column["Null"]=="NO" else "") if "NULL" in column else "", 
                ("DEFAULT({})".format(column["Default"]) if column["Default"]!="None" else "") if "Default" in column else "",
                column["Extra"] if "Extra" in column else "")
            sql += "PRIMARY KEY("
            
            
            for column in tableconfigs[t]:
                if "Key" in column:
                    if column["Key"]=="PRI":
                        sql += column["Field"] + ", "
            sql = sql[:-2] + ") )"
            
            ret += str(self.run(sql))
        return ret
        
    def insert(self, table, data):
        '''Inserts data in to the table.
        data: a dictionary or a list of dictionaries with keywords equal to column names.
        '''
        if type(data)==dict:
            data = [data]
        sql = "INSERT INTO `{}` (".format(table)
        for d in data[0]:
            sql += "`{}`, ".format(d)
        sql = sql[:-2] + ") VALUES "
        for l in data:
            sql+= "("
            for d in l:
                sql += "'{}', ".format(l[d])
            sql = sql[:-2] + "), "
        sql = sql[:-2]
        return self.run(sql)
    
    def select(self, table, where={}, wherenot={}, extra="", columns = ["*"]):
        '''Selects rows from the given tabel where the contitions in condition is met.
        Currently only is equal and not equal conditions work. Making less than and 
        grater than still requires handwritten SQL-code.

        By default gets all columns. Since you are working with dictionaries just pick
        what you need and ignore the rest. We are trying to be as Pythonic as possible 
        here. That is why the columns last. You can ignore them.
        
        Use extra for 'ORDER BY', 'LIMIT', and such.
       '''
        sql = "SELECT {} FROM `{}`".format(", ".join(columns), table)
        if where!={} or wherenot!={}:
            sql += " WHERE"
        if where!={}:
            for w in where:
                sql += " `{}`='{}' AND".format(w, where[w])
            sql = sql[:-4]
        if wherenot!={}:
            for w in wherenot:
                sql += " `{}`!='{}' AND".format(w, wherenot[w])
            sql = sql[:-4]
        return self.run(sql + " " + extra)

    def update(self, table, data, where={}, wherenot={}):
        '''Updates like a it is a combination of insert and select. 
        At least one of where and wherenot is required.'''

        sql = "UPDATE {} SET ".format(table)
        for d in data:
            sql += "`{}`='{}', ".format(d, data[d])
        sql = sql[:-2]
        sql += " WHERE"
        if where!={}:
            for w in where:
                sql += " `{}`='{}' AND".format(w, where[w])
            sql = sql[:-4]
        if wherenot!={}:
            for w in wherenot:
                sql += " `{}`!='{}' AND".format(w, wherenot[w])
            sql = sql[:-4]
        return self.run(sql)        

    def delete(self, table, where={}, wherenot={}):
        '''Deletes rows form the table where the conditions is met.
        
        DELETE FROM t1 WHERE c1
        '''
        sql = "DELETE FROM `{}` WHERE".format(table)
        if where!={}:
            for w in where:
                sql += " `{}`='{}' AND".format(w, where[w])
            sql = sql[:-4]
        if wherenot!={}:
            for w in wherenot:
                sql += " `{}`!='{}' AND".format(w, wherenot[w])
            sql = sql[:-4]
        return self.run(sql)

    def AlterTable(self, table, add=[], drop=[]):
        '''Alters a table'''
        sql = "ALTER TABLE `{}`".format(table)
        if type(drop)==str:
            drop = [drop]
        if drop!=[]:
            for c in drop:
                sql += " DROP COLUMN `{}`, ".format(c)
        if add==[]:
            sql = sql[:-2]
        if type(add)==dict:
            add = [add]
        if add!=[]:
            for a in add:
                sql += " ADD COLUMN IF NOT EXISTS "
                sql += "`{}` {} {} {} {}, ".format(a["Field"], 
                a["Type"], 
                ("NOT NULL" if a["Null"]=="NO" else "") if "NULL" in a else "", 
                ("DEFAULT({})".format(a["Default"]) if a["Default"]!="None" else "") if "Default" in a else "",
                a["Extra"] if "Extra" in a else "")
            sql = sql [:-2]
        #print(sql)
        return self.run(sql)

    '''Feed generators'''
    def FeedCreate(self, tableconfigs):
        '''Returns a feed for the create operation to be read by EatFeed() on another server.
        
        The feeds need to be put in a list afterwards.'''
        return {"operation":"create", "tableconfigs":tableconfigs}

    def FeedAlterTable(self, table, add=[], drop=[]):
        '''Returns a feed for the alter operation to be read by EatFeed() on another server.
        
        The feeds need to be put in a list afterwards.'''
        return {"operation":"alter table", "table":table, "add":add, "drop":drop}
        
    def FeedDrop(self, table):
        '''Returns a feed for the drop operation to be read by EatFeed() on another server.
        
        The feeds need to be put in a list afterwards.'''
        return {"operation":"drop", "table":table}
        
    def FeedInsert(self, table, data):
        '''Returns a feed for the insert operation to be read by EatFeed() on another server.
        
        The feeds need to be put in a list afterwards.'''
        return {"operation":"insert", "table":table, "data":data}

    def FeedUpdate(self, table, data, where={}, wherenot={}):
        '''Returns a feed for the update operation to be read by EatFeed() on another server.
        
        The feeds need to be put in a list afterwards.'''
        return {"operation":"update", "table":table, "data" : data, "where" : where, "wherenot" : wherenot }
    
    def FeedDelete(self, table, where={}, wherenot={}):
        '''Returns a feed for the delete operation to be read by EatFeed() on another server.
        
        The feeds need to be put in a list afterwards.'''
        return {"operation":"delete", "table": table, "where" : where, "wherenot" : wherenot }

    def GenerateFeed(self, feed):
        '''Generated a json string from the list of feeds in feed.
        This is the thing you are supposed to put in the feed for databass
        to eat on the other side. It contians the keyword "bassfeed". 
        Other then that you can add  whatever server information you like 
        to the json before you put it in the actuall feed.'''
        return json.dumps({"bassfeed":feed})

    '''Feed readers, becase a feed is bass food in this case'''
    def EatCreate(self, feed):
        return self.create(feed["tableconfigs"])

    def EatAlterTable(self, feed):
        table = feed["table"]
        add   = feed["add"]
        drop  = feed["drop"]
        return self.AlterTable(table, add, drop)

    def EatDrop(self, feed):
        return self.drop(feed["table"])

    def EatInsert(self, feed):
        table = feed["table"]
        data  = feed["data"]
        return self.insert(table, data)

    def EatUpdate(self, feed):
        table    = feed["table"]
        data     = feed["data"]
        where    = feed["where"]
        wherenot = feed["wherenot"]
        return self.update(table, data, where, wherenot)

    def EatDelete(self, feed):
        table    = feed["table"]
        where    = feed["where"]
        wherenot = feed["wherenot"]
        return self.delete(table, where, wherenot)

    def EatFeed(self, feed):
        '''This functions reads a feed, handles it and does operations 
        to the database.
        
        feed: a json string with atleast the keyword "bassfeed" in it.
        '''
        if "`" in feed:
            print()
            print("` in feed.")
            print("` is not alowed in feed.")
            print("Suspected SQL injection.")
            print("Suspected malicious feed.")
            print("Feed not proccessed.")
            print("-"*25)
            print(feed)
            print("-"*25)
            print()
            return False
        feeds = json.loads(feed)
        ret = ""
        for f in feeds["bassfeed"]:
            ret += str(self._feedeaters[f["operation"]](f)) + " "
        return ret
    
def shorten(data, maxlen=50):
    '''Shortens the contents of a list of dictionaries to make it 
    more eye friendly when printed with tabulate.
    data: A list of dictionaries
    '''
    ret=[]
    for i in data:
        r={}
        for j in i:
            r[j]=unicode(i[j])[:maxlen].replace("\n", " ")
        ret.append(r)
    return ret

def printrows(rows, format = "fancy_grid"):
    '''Pretty prints the list of dictionaries returned by databass.run()
    data: A list of dictionaries
    '''
    if type(rows)==list:
        if len(rows) > 0:
            print(tabulate([i.values() for i in rows], rows[0].keys(), format))
    else:
        print(rows)
    
