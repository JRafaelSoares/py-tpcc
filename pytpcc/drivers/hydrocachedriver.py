from drivers.abstractdriver import *
from cloudburst.client.client import CloudburstConnection
from cloudburst.shared.serializer import Serializer

from anna.lattices import MultiKeyCausalLattice
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
        return
    # End loadFinish

    # ------------------------------------------------------------------------
    # Pre-pocessing function for data loading
    # ------------------------------------------------------------------------
    def loadStart(self):
        return
    # End loadStart

    # ------------------------------------------------------------------------
    # Load tuples into a table for TPC-C benchmarking
    #
    # @param string table name
    # @param list of tuples corresponding to table schema
    # ------------------------------------------------------------------------
    def loadTuples(self, tableName, tuples):
        serializer = Serializer()
        if tableName == 'WAREHOUSE' :
            for row in tuples:
                base_key = 'WAREHOUSE:%s:' % row[0]
                self.cloudburst.kvs_client.put(base_key+"W_ID",
                                               serializer.dump_lattice(row[0], MultiKeyCausalLattice))
                self.cloudburst.kvs_client.put(base_key+"W_NAME",
                                               serializer.dump_lattice(row[1], MultiKeyCausalLattice))
                self.cloudburst.kvs_client.put(base_key+"W_STREET_1",
                                               serializer.dump_lattice(row[2], MultiKeyCausalLattice))
                self.cloudburst.kvs_client.put(base_key+"W_STREET_2",
                                               serializer.dump_lattice(row[3], MultiKeyCausalLattice))
                self.cloudburst.kvs_client.put(base_key+"W_CITY",
                                               serializer.dump_lattice(row[4], MultiKeyCausalLattice))
                self.cloudburst.kvs_client.put(base_key+"W_STATE",
                                               serializer.dump_lattice(row[5], MultiKeyCausalLattice))
                self.cloudburst.kvs_client.put(base_key+"W_ZIP",
                                               serializer.dump_lattice(row[6], MultiKeyCausalLattice))
                self.cloudburst.kvs_client.put(base_key+"W_TAX",
                                               serializer.dump_lattice(row[7], MultiKeyCausalLattice))
                self.cloudburst.kvs_client.put(base_key+"W_YTD",
                                               serializer.dump_lattice(row[8], MultiKeyCausalLattice))

        elif tableName == 'DISTRICT':
            return

        elif tableName == 'CUSTOMER':
            return

        elif tableName == 'HISTORY':
            return

        elif tableName == 'STOCK':
            return

        elif tableName == 'ORDERS':
            return

        elif tableName == 'NEW_ORDER':
            return

        elif tableName == 'ORDER_LINE':
            return

        elif tableName == 'ITEMS':
            return

    #lattice = serializer.dump_lattice(value, MultiKeyCausalLattice)
    #print("Inserting key " + key)
    #cloudburst.kvs_client.put(key, lattice)
    # End loadTuples

    # ------------------------------------------------------------------------
    # Return default configuration when none is specified via command line
    #
    # @return dictionary configuration parameters
    # ------------------------------------------------------------------------
    def makeDefaultConfig(self):
        return self.DEFAULT_CONFIG
    # End makeDefaultConfig()
