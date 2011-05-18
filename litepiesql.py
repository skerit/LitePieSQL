#!/usr/bin/env python
"""
Name: litepiesql.py
File Description: SQLite Wrapper for Python providing easy access to basic functions.
Author: Jelle De Loecker (skerit)
Inspired by: ricocheting's MySQL Wrapper for PHP
Web: http://www.kipdola.com/
Update: 2011-04-19
Version: 0.1
Copyright 2011 kipdola.com

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import itertools
import re
import time

class Database:
    
    type = ""
    
    def __init__(self, dbtype, db, host="none", port=0, login="none", passwrd="none"):
        """
            dbtype: sqlite, oracle
        """
        
        self.type = dbtype

        if dbtype == "sqlite":
            import sqlite3
            self.conn = sqlite3.connect(db)
            self.conn.row_factory = sqlite3.Row
        elif dbtype == "oracle":
            try:
                import cx_Oracle
            except ImportError:
                print "Error importing oracle"

            dsn = cx_Oracle.makedsn(host, port, db)
            self.conn = cx_Oracle.connect(login, passwrd, dsn) 
        
    def insert(self, tablename, data):
        """
        Insert data into a table.
        The data does not have to be escaped.
        """
        
        # Create a new cursor
        tc = self.conn.cursor()
        
        tablelist = ""
        valueholder = ""
        valuenr = 0
        valuelist = []
        valuedict = []
        
        for key, value in data.items():
            
            valuenr += 1
            
            if len(tablelist) > 0:
                tablelist += ', '
                valueholder += ', '
                
            # Add to table list
            tablelist += key
            
            # Add a holder
            if(self.type == "oracle"):
                valueholder += ':' + str(valuenr)
            else:
                valueholder += '?'
            
            # Look for the increment() function
            increment = re.match("^increment\((\-?\d+)\)$",str(value))

            if(self.type == "oracle"):
                if(str(value).lower() == 'null'):
                    valuelist[str(valuenr)] = None
                elif(str(value).lower() == 'now()'):
                    valuelist[str(valuenr)] = str(int(time.time()))
                else:
                    valuelist[str(valuenr)] = value
            else:
                if(str(value).lower() == 'null'):
                    valuelist.append(None)
                elif(str(value).lower() == 'now()'):
                    valuelist.append(str(int(time.time())))
                else:
                    valuelist.append(value)
                
        
        # Perform and commit the insert
        if(self.type == "oracle"):
            self.query("INSERT INTO " + tablename + " (" + tablelist + ") VALUES (" + valueholder + ");", valuedict)
        else:
            self.query("INSERT INTO " + tablename + " (" + tablelist + ") VALUES (" + valueholder + ");", valuelist)
        
        # Get the last inserted id
        id = self.query('SELECT last_insert_rowid();')[0]['last_insert_rowid()']
        
        # Close this connection
        tc.close()
        
        # Return the id
        return id
    
    def query(self, query, escapeList=None):
        """
        Perform a query. When an escapeList is provided it'll be used for
        variable substitution.
        
        Returns a list with dictionaries containing the result of your SELECT,
        or an empty list after an INSERT or UPDATE.
        """
        
        # Create a new cursor
        tc = self.conn.cursor()
        
        # Execute our query with or without values to escape
        if(escapeList):
            tc.execute(query, tuple(escapeList))
        else:
            tc.execute(query)
        
        # Make an empty result list
        result = []
    
        # A description is only set after a SELECT statement
        # Even when there are no results.
        if(tc.description):
            # Fetch the field names out of our cursor
            field_names = [d[0].lower() for d in tc.description]
            
            # Generate a dictionary
            while True:
                rows = tc.fetchmany()
                if not rows: break
                for row in rows:
                    result.append(dict(itertools.izip(field_names, row)))
        else:
            # If there is no description this must mean we're doing an insert
            # or update. Anything that needs a commit.
            self.conn.commit()
        
        # Close the cursor        
        tc.close()

        # Return the list with the dictionaries
        return result
    
    def update(self, tablename, data, where):
        """
        Update a table.
        The WHERE variable you give has to be escaped
        """
        
        # Create a new cursor
        tc = self.conn.cursor()
        
        # Store all the field names we'll be updating in order
        updatelist = ""
        
        # Add all the updates values to a list in order,
        # which we'll convert to a tuple when needed.
        valuelist = []
        
        for key, value in data.items():
            if len(updatelist) > 0:
                updatelist += ', '
            
            # Look for the increment() function
            increment = re.match("^increment\((\-?\d+)\)$",str(value))
            
            if(str(value).lower() == 'null'):
                updatelist += key + "=?"
                valuelist.append(None)
            elif(increment):
                updatelist += key + "=" + key + '+' + list(increment.groups(0))[0]
            elif(str(value).lower() == 'now()'):
                updatelist += key + "=?"
                valuelist.append(str(int(time.time())))
            else:
                updatelist += key + "=?"
                valuelist.append(value)
        
        self.query("UPDATE " + tablename + " SET " + updatelist + " WHERE " + where + ";", valuelist)
        
        tc.close()
    
    def truncate(self, tablename):
        """
        Delete all rows from a table and reset the autoincrement
        """
        
        # Create a new cursor
        tc = self.conn.cursor()
        
        # Clear the table
        tc.execute("delete from "+ tablename + ";")
        
        # Reset the autoincrement
        tc.execute("delete from sqlite_sequence where name='"+ tablename + "';")
        
        tc.close()

