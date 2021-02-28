import time
import logging
import sys
from drivers.abstractdriver import *
from cloudburst.client.client import CloudburstConnection
from cloudburst.shared.serializer import Serializer
from cloudburst.shared.reference import CloudburstReference
from anna.lattices import MultiKeyCausalLattice
from cloudburst.shared.proto.cloudburst_pb2 import (
    Continuation,
    DagTrigger,
    FunctionCall,
    NORMAL, MULTI,  # Cloudburst's consistency modes,
    EXECUTION_ERROR, FUNC_NOT_FOUND,  # Cloudburst's error types
    MULTIEXEC # Cloudburst's execution types
)
import constants

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

#----------------------------------------------------------------------------
# Hydrocache TPC-C Driver
#
# Requires Cloudburst, Anna-Cache (Hydrocache) and Anna
# @author Rafael Soares <joao.rafael.pinto.soares@tecnico.ulisboa.pt>
#----------------------------------------------------------------------------
class HydrocacheDriver(AbstractDriver):

    DEFAULT_CONFIG = {
        'func_address' : ("Address of the Cloudburst Interface", "127.0.0.1"),
        'client_ip': ("IP of the client address", "127.0.0.1"),
        'client_id': ("Client unique id", 0),
        'local': ("Flag for local run", True),
        'host-info': ("Show information about hosts", 'Verbose'),
        'debug-load': ("Show Loading Information", 'None'),
        'debug-delivery': ("Show Delivery Performance", 'None'),
        'debug-new-order': ("Show New Order Performance", 'None'),
        'debug-order-status': ("Show Order Status Performance", 'None'),
        'debug-payment': ("Show Payment Performance", 'None'),
        'debug-stock-level': ("Show Stock Level Performance", 'None'),
    }
    # ------------------------------------------------------------------------
    # Class constructor
    #
    # @param string ddl (Data Definintion Language)
    # ------------------------------------------------------------------------
    def __init__(self, ddl):
        super(HydrocacheDriver,self).__init__("hydrocache",ddl)
        self.cloudburst = None
        self.metadata = {}
        self.t0 = 0
        self.debug = {
            'load': 'True',
            'delivery': 'None',
            'new-order': 'None',
            'order-status': 'None',
            'payment': 'None',
            'stock-level': 'None',
        }
    # End __init__()

    # ------------------------------------------------------------------------
    # Execute TPC-C Delivery Transaction
    #
    # @param dictionary params (transaction parameters)
    #	{
    #		"w_id"          : value,
    #		"o_carrier_id"  : value,
    #		"ol_delivery_d" : value,
    #	}
    # ------------------------------------------------------------------------
    def doDelivery(self, params):
        return
    # End doDelivery()

    # ------------------------------------------------------------------------
    # Execute TPC-C New Order Transaction
    #
    # @param dictionary params (transaction parameters)
    #	{
    #		'w_id' : value,
    #		'd_id' : value,
    #		'c_id' : value,
    #		'o_entry_d' : value,
    #		'i_ids' : value,
    #		'i_w_ids' : value,
    #		'i_qtys' : value,
    #	}
    #
    # ------------------------------------------------------------------------
    def doNewOrder(self, params):
        if self.debug['new-order'] != 'None':
            logging.info('TXN NEW ORDER STARTING -----------------')
            tt = time.time()
        if self.debug['new-order'] == 'Verbose':
            t0 = tt

        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]

        # Validate transaction parameters
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        # Define transaction arguments
        args = []

        # ------------------------------------------------------------
        # Obtains All Item Information
        # ------------------------------------------------------------
        args.append(i_ids)
        all_local = True
        items = []
        for i in range(len(i_ids)):
            all_local = all_local and i_w_ids[i] == w_id
            item_key = 'ITEM.%s.' % str(i_ids[i])
            items.append(CloudburstReference(item_key + 'I_PRICE', True))
            items.append(CloudburstReference(item_key + 'I_NAME', True))
            items.append(CloudburstReference(item_key + 'I_DATA', True))
        args.append(items)
        args.append(all_local)

        # ------------------------------
        # Get Warehouse Tax Rate Query
        # ------------------------------
        warehouse_key = "WAREHOUSE.%s." % w_id
        args.append(CloudburstReference(warehouse_key + 'W_TAX', True))

        # ------------------------------------------
        # Get District Tax And Next Order ID Query
        # ------------------------------------------
        district_key = "DISTRICT.%s.%s." % (w_id, d_id)
        args.append(CloudburstReference(district_key + "D_TAX", True))
        args.append(CloudburstReference(district_key + "D_NEXT_O_ID", True))

        # ------------------------------------------
        # Get Client Information And Discount
        # ------------------------------------------
        customer_info = []
        customer_key = "CUSTOMER.%s.%s.%s." % (w_id, d_id, c_id)
        args.append(customer_info)
        args.append(CloudburstReference(customer_key + "C_DISCOUNT", True))

        # --------------------------------
        # Get Stock Information Query
        # --------------------------------
        stocks = []
        for i in range(len(i_ids)):
            stock_key = "STOCK.%s.%s." % (i_w_ids[i], i_ids[i])
            stocks.append(CloudburstReference(stock_key + "S_QUANTITY", True))
            stocks.append(CloudburstReference(stock_key + "S_YTD", True))
            stocks.append(CloudburstReference(stock_key + "S_ORDER_CNT", True))
            stocks.append(CloudburstReference(stock_key + "S_REMOTE_CNT", True))
            stocks.append(CloudburstReference(stock_key + "S_DATA", True))
            if len(str(d_id)) == 1:
                stocks.append(CloudburstReference(stock_key + 'S_DIST_0' + str(d_id), True))
            else:
                stocks.append(CloudburstReference(stock_key + 'S_DIST_' + str(d_id), True))
        args.extend(stocks)

        dag_name = "doNewOrderDag"
        request = {"doNewOrder": args}
        result = self.cloudburst.call_dag(dag_name, request, consistency=MULTI, output_key="output_key", direct_response=True)

        if self.debug['new-order'] != 'None':
            logging.info('TXN NEW ORDER FINISHED -----------------')
            logging.info('EXECUTION TIME: %s', time.time() - tt)

        return result

    # End doNewOrder

    # ------------------------------------------------------------------------
    # Execute TPC-C Do Order Status transaction
    #
    # @param dictionary params (transaction parameters)
    #	{
    #		'w_id'   : value,
    #		'd_id'   : value,
    #		'c_id'   : value,
    #		'c_last' : value,
    #	}
    # ------------------------------------------------------------------------
    def doOrderStatus(self, params):
        return
    # End doOrderStatus

    # ------------------------------------------------------------------------
    # Execute TPC-C Do Payement Transaction
    #
    # @param dictionary params (transaction parameters)
    #	{
    #		'w_id'     : value,
    #		'd_id'     : value,
    #		'h_amount' : value,
    #		'c_w_id'   : value,
    #		'c_d_id'   : value,
    #		'c_id'     : value,
    #		'c_last'   : value,
    #		'h_date'   : value,
    #	}
    # ------------------------------------------------------------------------
    def doPayment(self, params):
        return
    # End doPayment

    # ------------------------------------------------------------------------
    # Execute TPC-C Stock Level Transaction
    #
    # @param dictionary params (transaction parameters)
    #	{
    #		'w_id'     : value,
    #		'd_id'     : value,
    #		'threshold : value,
    #	}
    # ------------------------------------------------------------------------
    def doStockLevel(self, params):
        return
    # End doStockLevel

    # ------------------------------------------------------------------------
    # Load the specified configuration for Cloudburst TPC-C run
    #
    # @param dictionary config (configuration options)
    # ------------------------------------------------------------------------
    def loadConfig(self, config):
        for key in HydrocacheDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)

        func_address = config['func_address']
        client_ip = config['client_ip']
        client_id = config['client_id']
        local = config['local']

        self.cloudburst = CloudburstConnection(func_address, client_ip, client_id, local)
        if config['reset']:
            #TODO - Flush all Anna database
            print("asd")
        return
    # End loadConfig

    # ------------------------------------------------------------------------
    # Post-processing function for data loading
    # ------------------------------------------------------------------------
    def loadFinish(self):
        elapsed = time.time() - self.t0
        logging.info('Loading Complete: ' + str(elapsed) + ' elapsed')

        # Store Metadata
        for table, next in self.next_scores.items():
            self.metadata[table + '.next_score'] = next

        self.cloudburst.kvs_client.put("output_key", self.getKeyLattice(0))
        self.cloudburst.register(doNewOrder, "doNewOrder")
        self.cloudburst.register_dag("doNewOrderDag", ["doNewOrder"], [])
    # End loadFinish

    # ------------------------------------------------------------------------
    # Pre-pocessing function for data loading
    # ------------------------------------------------------------------------
    def loadStart(self):
        if self.debug['load'] != 'None':
            logging.info('Starting data load')
        self.t0 = time.time()

        # Used for Number of orders and History ID

        self.next_scores = {
            'WAREHOUSE': 1,
            'DISTRICT': 1,
            'ITEM': 1,
            'CUSTOMER': 1,
            'HISTORY': 1,
            'STOCK': 1,
            'ORDERS': 1,
            'NEW_ORDER': 1,
            'ORDER_LINE': 1,
        }
    # End loadStart

    # ------------------------------------------------------------------------
    # Load tuples into a table for TPC-C benchmarking
    #
    # @param string table name
    # @param list of tuples corresponding to table schema
    # ------------------------------------------------------------------------
    def loadTuples(self, tableName, tuples):

        if self.debug['load'] != 'None':
            logging.info("Loading %s" % tableName)

        if tableName == 'WAREHOUSE':
            for row in tuples:
                base_key = 'WAREHOUSE.%s.' % row[0]
                self.cloudburst.kvs_client.put(base_key+"W_ID", self.getKeyLattice(row[0]))
                self.cloudburst.kvs_client.put(base_key+"W_NAME", self.getKeyLattice(row[1]))
                self.cloudburst.kvs_client.put(base_key+"W_STREET_1", self.getKeyLattice(row[2]))
                self.cloudburst.kvs_client.put(base_key+"W_STREET_2", self.getKeyLattice(row[3]))
                self.cloudburst.kvs_client.put(base_key+"W_CITY", self.getKeyLattice(row[4]))
                self.cloudburst.kvs_client.put(base_key+"W_STATE", self.getKeyLattice(row[5]))
                self.cloudburst.kvs_client.put(base_key+"W_ZIP", self.getKeyLattice(row[6]))
                self.cloudburst.kvs_client.put(base_key+"W_TAX", self.getKeyLattice(row[7]))
                self.cloudburst.kvs_client.put(base_key+"W_YTD", self.getKeyLattice(row[8]))

        elif tableName == 'DISTRICT':
            for row in tuples:
                base_key = 'DISTRICT.%s.%s.' % (row[1], row[0])
                self.cloudburst.kvs_client.put(base_key + "D_ID", self.getKeyLattice(row[0]))
                self.cloudburst.kvs_client.put(base_key + "D_W_ID", self.getKeyLattice(row[1]))
                self.cloudburst.kvs_client.put(base_key + "D_NAME", self.getKeyLattice(row[2]))
                self.cloudburst.kvs_client.put(base_key + "D_STREET_1", self.getKeyLattice(row[3]))
                self.cloudburst.kvs_client.put(base_key + "D_STREET_2", self.getKeyLattice(row[4]))
                self.cloudburst.kvs_client.put(base_key + "D_CITY", self.getKeyLattice(row[5]))
                self.cloudburst.kvs_client.put(base_key + "D_STATE", self.getKeyLattice(row[6]))
                self.cloudburst.kvs_client.put(base_key + "D_ZIP", self.getKeyLattice(row[7]))
                self.cloudburst.kvs_client.put(base_key + "D_TAX", self.getKeyLattice(row[8]))
                self.cloudburst.kvs_client.put(base_key + "D_YTD", self.getKeyLattice(row[9]))
                self.cloudburst.kvs_client.put(base_key + "D_NEXT_O_ID", self.getKeyLattice(row[10]))

        elif tableName == 'CUSTOMER':
            for row in tuples:
                base_key = 'CUSTOMER.%s.%s.%s.' % (row[2], row[1], row[0])
                self.cloudburst.kvs_client.put(base_key + "C_ID", self.getKeyLattice(row[0]))
                self.cloudburst.kvs_client.put(base_key + "C_D_ID", self.getKeyLattice(row[1]))
                self.cloudburst.kvs_client.put(base_key + "C_W_ID", self.getKeyLattice(row[2]))
                self.cloudburst.kvs_client.put(base_key + "C_FIRST", self.getKeyLattice(row[3]))
                self.cloudburst.kvs_client.put(base_key + "C_MIDDLE", self.getKeyLattice(row[4]))
                self.cloudburst.kvs_client.put(base_key + "C_LAST", self.getKeyLattice(row[5]))
                self.cloudburst.kvs_client.put(base_key + "C_STREET_1", self.getKeyLattice(row[6]))
                self.cloudburst.kvs_client.put(base_key + "C_STREET_2", self.getKeyLattice(row[7]))
                self.cloudburst.kvs_client.put(base_key + "C_CITY", self.getKeyLattice(row[8]))
                self.cloudburst.kvs_client.put(base_key + "C_ZIP", self.getKeyLattice(row[9]))
                self.cloudburst.kvs_client.put(base_key + "C_PHONE", self.getKeyLattice(row[10]))
                self.cloudburst.kvs_client.put(base_key + "C_SINCE", self.getKeyLattice(row[11]))
                self.cloudburst.kvs_client.put(base_key + "C_CREDIT", self.getKeyLattice(row[12]))
                self.cloudburst.kvs_client.put(base_key + "C_CREDIT_LIM", self.getKeyLattice(row[13]))
                self.cloudburst.kvs_client.put(base_key + "C_DISCOUNT", self.getKeyLattice(row[14]))
                self.cloudburst.kvs_client.put(base_key + "C_BALANCE", self.getKeyLattice(row[15]))
                self.cloudburst.kvs_client.put(base_key + "C_YTD_PAYMENT", self.getKeyLattice(row[16]))
                self.cloudburst.kvs_client.put(base_key + "C_PAYMENT_CNT", self.getKeyLattice(row[17]))
                self.cloudburst.kvs_client.put(base_key + "C_DELIVERY_CNT", self.getKeyLattice(row[18]))
                self.cloudburst.kvs_client.put(base_key + "C_DATA", self.getKeyLattice(row[19]))

        elif tableName == 'HISTORY':
            for row in tuples:
                base_key = 'HISTORY.%s.' % self.next_scores['HISTORY']
                self.cloudburst.kvs_client.put(base_key + "H_C_ID", self.getKeyLattice(row[0]))
                self.cloudburst.kvs_client.put(base_key + "H_C_D_ID", self.getKeyLattice(row[1]))
                self.cloudburst.kvs_client.put(base_key + "H_C_W_ID", self.getKeyLattice(row[2]))
                self.cloudburst.kvs_client.put(base_key + "H_D_ID", self.getKeyLattice(row[3]))
                self.cloudburst.kvs_client.put(base_key + "H_W_ID", self.getKeyLattice(row[4]))
                self.cloudburst.kvs_client.put(base_key + "H_DATE", self.getKeyLattice(row[5]))
                self.cloudburst.kvs_client.put(base_key + "H_AMOUNT", self.getKeyLattice(row[6]))
                self.cloudburst.kvs_client.put(base_key + "H_DATA", self.getKeyLattice(row[7]))

        elif tableName == 'STOCK':
            for row in tuples:
                base_key = 'STOCK.%s.%s.' % (row[1], row[0])
                self.cloudburst.kvs_client.put(base_key + "S_I_ID", self.getKeyLattice(row[0]))
                self.cloudburst.kvs_client.put(base_key + "S_W_ID", self.getKeyLattice(row[1]))
                self.cloudburst.kvs_client.put(base_key + "S_QUANTITY", self.getKeyLattice(row[2]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_01", self.getKeyLattice(row[3]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_02", self.getKeyLattice(row[4]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_03", self.getKeyLattice(row[5]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_04", self.getKeyLattice(row[6]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_05", self.getKeyLattice(row[7]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_06", self.getKeyLattice(row[8]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_07", self.getKeyLattice(row[9]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_08", self.getKeyLattice(row[10]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_09", self.getKeyLattice(row[11]))
                self.cloudburst.kvs_client.put(base_key + "S_DIST_10", self.getKeyLattice(row[12]))
                self.cloudburst.kvs_client.put(base_key + "S_YTD", self.getKeyLattice(row[13]))
                self.cloudburst.kvs_client.put(base_key + "S_ORDER_CNT", self.getKeyLattice(row[14]))
                self.cloudburst.kvs_client.put(base_key + "S_REMOTE_CNT", self.getKeyLattice(row[15]))
                self.cloudburst.kvs_client.put(base_key + "S_DATA", self.getKeyLattice(row[16]))

        elif tableName == 'ORDERS':
            for row in tuples:
                base_key = 'ORDER.%s.%s.%s.%s.' % (row[0], row[3], row[1], row[2])
                self.cloudburst.kvs_client.put(base_key + "O_ID", self.getKeyLattice(row[0]))
                self.cloudburst.kvs_client.put(base_key + "O_D_ID", self.getKeyLattice(row[1]))
                self.cloudburst.kvs_client.put(base_key + "O_W_ID", self.getKeyLattice(row[2]))
                self.cloudburst.kvs_client.put(base_key + "O_C_ID", self.getKeyLattice(row[3]))
                self.cloudburst.kvs_client.put(base_key + "O_ENTRY_D", self.getKeyLattice(row[4]))
                self.cloudburst.kvs_client.put(base_key + "O_CARRIER_ID", self.getKeyLattice(row[5]))
                self.cloudburst.kvs_client.put(base_key + "O_OL_CNT", self.getKeyLattice(row[6]))
                self.cloudburst.kvs_client.put(base_key + "O_ALL_LOCAL", self.getKeyLattice(row[7]))

        elif tableName == 'NEW_ORDER':
            for row in tuples:
                base_key = 'NEW_ORDER.%s.%s.%s.' % (row[1], row[2], row[0])
                self.cloudburst.kvs_client.put(base_key + "NO_O_ID", self.getKeyLattice(row[0]))
                self.cloudburst.kvs_client.put(base_key + "NO_D_ID", self.getKeyLattice(row[1]))
                self.cloudburst.kvs_client.put(base_key + "NO_W_ID", self.getKeyLattice(row[2]))

        elif tableName == 'ORDER_LINE':
            for row in tuples:
                base_key = 'ORDER_LINE.%s.%s.%s.%s.' % (row[2], row[1], row[0], row[3])
                self.cloudburst.kvs_client.put(base_key + "OL_O_ID", self.getKeyLattice(row[0]))
                self.cloudburst.kvs_client.put(base_key + "OL_D_ID", self.getKeyLattice(row[1]))
                self.cloudburst.kvs_client.put(base_key + "OL_W_ID", self.getKeyLattice(row[2]))
                self.cloudburst.kvs_client.put(base_key + "OL_NUMBER", self.getKeyLattice(row[3]))
                self.cloudburst.kvs_client.put(base_key + "OL_I_ID", self.getKeyLattice(row[4]))
                self.cloudburst.kvs_client.put(base_key + "OL_SUPPLY_W_ID", self.getKeyLattice(row[5]))
                self.cloudburst.kvs_client.put(base_key + "OL_DELIVERY_D", self.getKeyLattice(row[6]))
                self.cloudburst.kvs_client.put(base_key + "OL_QUANTITY", self.getKeyLattice(row[7]))
                self.cloudburst.kvs_client.put(base_key + "OL_AMOUNT", self.getKeyLattice(row[8]))
                self.cloudburst.kvs_client.put(base_key + "OL_DIST_INFO", self.getKeyLattice(row[9]))

        elif tableName == 'ITEMS':
            for row in tuples:
                base_key = 'ITEM.%s.' % row[0]
                self.cloudburst.kvs_client.put(base_key + "I_ID", self.getKeyLattice(row[0]))
                self.cloudburst.kvs_client.put(base_key + "I_IM_ID", self.getKeyLattice(row[1]))
                self.cloudburst.kvs_client.put(base_key + "I_NAME", self.getKeyLattice(row[2]))
                self.cloudburst.kvs_client.put(base_key + "I_PRICE", self.getKeyLattice(row[3]))
                self.cloudburst.kvs_client.put(base_key + "I_DATA", self.getKeyLattice(row[4]))

        self.next_scores[tableName] += 1

    # End loadTuples

    # ------------------------------------------------------------------------
    # Return default configuration when none is specified via command line
    #
    # @return dictionary configuration parameters
    # ------------------------------------------------------------------------
    def makeDefaultConfig(self):
        return self.DEFAULT_CONFIG
    # End makeDefaultConfig()

    # ------------------------------------------------------------------------
    # Aux Functions
    # ------------------------------------------------------------------------

    # ------------------------------------------------------------------------
    # Aux function to return the serialized Lattice to insert into the KVS
    # Allow us to easily change the desired Lattice if needed
    #
    # @return Serialized Lattice MultiKeyCausalLattice (Causal Lattice)
    # ------------------------------------------------------------------------
    def getKeyLattice(self, value):
        serializer = Serializer()
        return serializer.dump_lattice(value, MultiKeyCausalLattice)

# ------------------------------------------------------------------------
# doNewOrder transaction to be registered and executed by cloudburst
#
#   Arg_list:
#       params - Parameters seen on top
#       ITEMS: - i_ids number of iteems
#           ITEM.i_id.I_PRICE
#           ITEM.i_id.I_NAME
#           ITEM.i_id.I_DATA
#       all_local - flag if all items are local
#       WAREHOUSE.w_id.W_TAX - Tax value of warehouse
#       DISTRICT.w_id.d_id.D_TAX - Tax value of district
#       DISTRICT.w_id.d_id.D_NEXT_O_ID - District next ordered ID
#       CLIENT_INFO:
#           all information of client
#       CLIENT.w_id.d_id.c_id.C_DISCOUNT - Client Discount
#       STOCKS: - List of Stocks
#           STOCK.i_w_id.i_id.S_QUANTITY
#           STOCK.i_w_id.i_id.S_YTD
#           STOCK.i_w_id.i_id.S_ORDER_CNT
#           STOCK.i_w_id.i_id.S_REMOTE_CNT
#           STOCK.i_w_id.i_id.S_DATA
#           STOCK.i_w_id.i_id.S_DIST
#
# @return
# ------------------------------------------------------------------------


def doNewOrder(cloudburst, write_set, params, items, all_local, w_tax, d_tax, d_next_o_id, customer_info, c_discount, stocks):
    w_id = params["w_id"]
    d_id = params["d_id"]
    c_id = params["c_id"]
    o_entry_d = params["o_entry_d"]
    i_ids = params["i_ids"]
    i_w_ids = params["i_w_ids"]
    i_qtys = params["i_qtys"]

    # -------------------------------
    # Increment Next Order ID Query
    # -------------------------------
    district_key = "DISTRICT.%s.%s." % (w_id, d_id)
    district_next_order_id_key = district_key + 'D_NEXT_O_ID'
    cloudburst.write(write_set, district_next_order_id_key, d_next_o_id+1)

    # --------------------
    # Create Order Query
    # --------------------
    order_key = "ORDERS.%s.%s.%s." % (w_id, d_id, d_next_o_id)
    ol_cnt = len(i_ids)
    cloudburst.write(write_set, order_key + "O_ID", d_next_o_id)
    cloudburst.write(write_set, order_key + "O_D_ID", d_id)
    cloudburst.write(write_set, order_key + "O_W_ID", w_id)
    cloudburst.write(write_set, order_key + "O_C_ID", c_id)
    cloudburst.write(write_set, order_key + "O_C_ID", c_id)
    cloudburst.write(write_set, order_key + "O_ENTRY_D", o_entry_d)
    cloudburst.write(write_set, order_key + "O_CARRIER_ID", constants.NULL_CARRIER_ID)
    cloudburst.write(write_set, order_key + "O_OL_CNT", ol_cnt)
    cloudburst.write(write_set, order_key + "O_ALL_LOCAL", all_local)

    # Add index for Order searching
    # si_key = self.safeKey([w_id, d_id, c_id])
    # wtr.sadd('ORDERS.INDEXES.ORDERSEARCH', si_key)
    # TODO - Do we need this?

    # ------------------------
    # Create New Order Query
    # ------------------------
    new_order_key = "NEW_ORDER.%s.%s.%s." % (d_next_o_id, w_id, d_id)
    cloudburst.write(write_set, new_order_key + "NO_O_ID", d_next_o_id)
    cloudburst.write(write_set, new_order_key + "NO_D_ID", d_id)
    cloudburst.write(write_set, new_order_key + "NO_W_ID", w_id)

    # Add index for New Order Searching
    # si_key = self.safeKey([d_id, w_id])
    # wtr.sadd('NEW_ORDER.INDEXES.GETNEWORDER', si_key)
    # TODO - Do we need this?

    # -------------------------------
    # Insert Order Item Information
    # -------------------------------
    item_data = []
    total = 0

    ol_number = []
    ol_quantity = []
    ol_supply_w_id = []
    ol_i_id = []
    i_name = []
    i_price = []
    i_data = []
    stock_key = []

    for i in range(len(i_ids)):
        ol_number.append(i + 1)
        ol_supply_w_id.append(i_w_ids[i])
        ol_i_id.append(i_ids[i])
        ol_quantity.append(i_qtys[i])

        i_price.append(float(items[i*3]))
        i_name.append(items[i*3 + 1])
        i_data.append(items[i*3 + 2])
        stock_key.append("%s.%s." % (ol_supply_w_id[i], ol_i_id[i]))

    # We divide by 6 since for each stock we obtained 6 keys
    for i in range(len(stocks) // 6):
        s_quantity = float(stocks[i*6])
        s_ytd = float(stocks[i*6 + 1])
        s_order_cnt = float(stocks[i*6 + 2])
        s_remote_cnt = float(stocks[i*6 + 3])
        s_data = stocks[i*6 + 4]
        s_dist_xx = stocks[i*6 + 5]

        s_ytd += ol_quantity[i]

        if s_quantity >= ol_quantity[i] + 10:
            s_quantity = s_quantity - ol_quantity[i]
        else:
            s_quantity = s_quantity + 91 - ol_quantity[i]
        s_order_cnt += 1

        if ol_supply_w_id[i] != w_id:
            s_remote_cnt += 1

        current_stock_key = "STOCK.%s." % stock_key[i]

        cloudburst.write(write_set, current_stock_key + "S_QUANTITY", s_quantity)
        cloudburst.write(write_set, current_stock_key + "S_YTD", s_ytd)
        cloudburst.write(write_set, current_stock_key + "S_ORDER_CNT", s_order_cnt)
        cloudburst.write(write_set, current_stock_key + "S_REMOTE_CNT", s_remote_cnt)

        if i_data[i].find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
            brand_generic = 'B'
        else:
            brand_generic = 'G'

        ## Transaction profile states to use "ol_quantity * i_price"
        ol_amount = ol_quantity[i] * i_price[i]
        total += ol_amount

        # -------------------------
        # Create Order Line Query
        # -------------------------
        order_line_key = "ORDER_LINE.%s.%s.%s.%s." % (w_id, d_id, d_next_o_id, ol_number)

        cloudburst.write(write_set, order_line_key + "OL_O_ID", d_next_o_id)
        cloudburst.write(write_set, order_line_key + "OL_D_ID", d_id)
        cloudburst.write(write_set, order_line_key + "OL_W_ID", w_id)
        cloudburst.write(write_set, order_line_key + "OL_NUMBER", ol_number[i])
        cloudburst.write(write_set, order_line_key + "OL_I_ID", ol_i_id[i])
        cloudburst.write(write_set, order_line_key + "OL_SUPPLY_W_ID", ol_supply_w_id[i])
        cloudburst.write(write_set, order_line_key + "OL_DELIVERY_D", o_entry_d)
        cloudburst.write(write_set, order_line_key + "OL_QUANTITY", ol_quantity[i])
        cloudburst.write(write_set, order_line_key + "OL_AMOUNT", ol_amount)
        cloudburst.write(write_set, order_line_key + "OL_DISTRICT_INFO", s_dist_xx)

        # Create index for Order Line Searching
        # wtr.sadd(
        #    'ORDER_LINE.INDEXES.SUMOLAMOUNT.' + si_key,
        #   order_line_key
        # )
        # TODO - Is this necessary?

        item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
    ## End for i in range(len(stocks) // 6):

    ## Adjust the total for the discount
    total *= (1 - c_discount) * (1 + w_tax + d_tax)

    ## Pack up values the client is missing (see TPC-C 2.4.3.5)
    misc = [(w_tax, d_tax, d_next_o_id, total)]

    return [customer_info, misc, item_data]
# End doNewOrderDag
