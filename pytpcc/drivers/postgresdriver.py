# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Adapted to PostgreSQL by jop.
#
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

from __future__ import with_statement

import os
import psycopg2
import logging
import commands
from pprint import pprint,pformat

import constants
from abstractdriver import *

TXN_QUERIES = {
    
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_LAST = %s ORDER BY C_FIRST", # w_id, d_id, c_last
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s", # w_id, d_id, o_id        
    },
        
    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = %s AND D_ID = %s", 
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = %s
              AND OL_D_ID = %s
              AND OL_O_ID < %s
              AND OL_O_ID >= %s
              AND S_W_ID = %s
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < %s
        """,
    },
}


## ==============================================
## PostgresDriver
## ==============================================
class PostgresDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "database": ("The connection string to the PostgreSQL database", "host=localhost dbname=tpcc" ),
    }
    
    def __init__(self, ddl):
        super(PostgresDriver, self).__init__("postgres", ddl)
        self.database = None
        self.conn = None
        self.cursor = None
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return PostgresDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in PostgresDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.database = str(config["database"])
                    
        self.conn = psycopg2.connect(self.database)
        self.cursor = self.conn.cursor()

        if config["reset"]:
            logging.debug("Deleting database '%s'" % self.database)
            self.cursor.execute("DROP SCHEMA public CASCADE")
            self.cursor.execute("CREATE SCHEMA public")
            self.cursor.execute("DROP DOMAIN IF EXISTS TINYINT")

        self.cursor.execute("select * from information_schema.tables where table_name=%s", ('order_line',))
        if self.cursor.rowcount <= 0:
            logging.debug("Loading DDL file '%s'" % (self.ddl))
            self.cursor.execute("CREATE DOMAIN TINYINT AS SMALLINT")
            self.cursor.execute(open(self.ddl, "r").read())

    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        
        p = ["%s"]*len(tuples[0])
        sql = "INSERT INTO %s VALUES (%s)" % (tableName, ",".join(p))
        self.cursor.executemany(sql, tuples)
        
        logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
        return

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Commiting changes to database")
        self.conn.commit()

    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        q = TXN_QUERIES["ORDER_STATUS"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1)/2
            customer = all_customers[index]
            c_id = customer[0]
        assert len(customer) > 0
        assert c_id != None

        self.cursor.execute(q["getLastOrder"], [w_id, d_id, c_id])
        order = self.cursor.fetchone()
        if order:
            self.cursor.execute(q["getOrderLines"], [w_id, d_id, order[0]])
            orderLines = self.cursor.fetchall()
        else:
            orderLines = [ ]

        self.conn.commit()
        return [ customer, order, orderLines ]
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        q = TXN_QUERIES["STOCK_LEVEL"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        self.cursor.execute(q["getOId"], [w_id, d_id])
        result = self.cursor.fetchone()
        assert result
        o_id = result[0]
        
        self.cursor.execute(q["getStockCount"], [w_id, d_id, o_id, (o_id - 20), w_id, threshold])
        result = self.cursor.fetchone()
        
        self.conn.commit()
        
        return int(result[0])
        
## CLASS
