'''Pyhon class for accesing the a database by returning a list of dictionaries from a query.
Works great in combination with the tabulate package.

Copyright (C) 2018 Lord Wolfenstein
'''
import mysql.connector

class databass:
    '''Class that simplifyes database connections.'''

    def __init__(self, config):
        '''Config format
        config             = {}
        config["user"]     = "root"
        config["password"] = "pass"
        config["host"]     = "127.0.0.1"
        config["port"]     = "3306"
        config["database"] = "test"
        '''
        self.databas = mysql.connector.connect(**config)
        self.cursor  = self.databas.cursor()

    def run(self, sql):
        '''Runs a query.
           Returns a list of dictionaries on successfull SELECT.
        '''
        try:
            self.cursor.execute(sql)
        except mysql.connector.Error as err:
            return "Database Error: " + str(err)
        names = self.cursor.description
        if names:
            # Must have been a SELECT I think
            n = []
            for i in names:
                n.append(i[0])
                ret = []
            for row in self.cursor:
                r = {}
                i = 0
                for column in row:
                    if type(column)==unicode or type(column) == int or type(column) == str:
                        r[n[i]] = column
                    else:
                        r[n[i]] = str(column)
                    i += 1
                ret.append(r)
            self.databas.commit()
            return ret
        else:
            # It modyfied the database and that needs to be commited
            self.databas.commit()
            return True

    def count(self, table):
        '''Returns the number of rows in a given table.'''
        return self.run("SELECT count(*) FROM " + table)[0]["count(*)"]

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
        return self.run("DESCRIBE " + table)

    def code(self, table):
        '''Returns the code used to create the table'''
        return self.run("SHOW CREATE TABLE " + table)[0]["Create Table"]

def shorten(data, maxlen=50):
	'''Shortens the contents of a list of dictionaries to make it more eye friendly when printed with tabulate.
	   data: A list of dictionaries
	'''
	ret=[]
	for i in data:
		r={}
		for j in i:
			r[j]=unicode(i[j])[:maxlen].replace("\n", " ")
		ret.append(r)
	return ret
