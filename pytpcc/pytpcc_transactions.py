import datetime

local = True # or False if you are running against a HydroCluster
elb_address = '127.0.0.1' # or the address of the ELB returned by the
from cloudburst.client.client import CloudburstConnection
from cloudburst.shared.serializer import Serializer
from anna.lattices import MultiKeyCausalLattice
from cloudburst.shared.reference import CloudburstReference
from cloudburst.server.benchmarks.ZipfGenerator import ZipfGenerator
from cloudburst.shared.proto.cloudburst_pb2 import (
    Continuation,
    DagTrigger,
    FunctionCall,
    NORMAL, MULTI,  # Cloudburst's consistency modes,
    EXECUTION_ERROR, FUNC_NOT_FOUND,  # Cloudburst's error types
    MULTIEXEC # Cloudburst's execution types
)
import time

# doNewOrderName
doNewOrderDagName = 'doNewOrderDag'
doNewOrderFunctionName = 'doNewOrderFunction'
connections = []
functions = [doNewOrderFunctionName]

# doDelivery Names
doDeliveryDagName = 'doDeliveryDag'
getNewOrderIndexName = 'getNewOrderIndex'
getNewOrdersName = 'getNewOrders'
getCustomerIDName = 'getCustomerID'
getOrderLineSumName = 'getOrderLineSum'
doDeliveryFunctionName = 'doDeliveryFunction'
doDeliveryFunctions = [getNewOrderIndexName, getNewOrdersName, getCustomerIDName, getOrderLineSumName,
                       doDeliveryFunctionName]
doDeliveryConnections = [(getNewOrderIndexName, getNewOrdersName), (getNewOrdersName, getCustomerIDName),
                         (getCustomerIDName, getOrderLineSumName), (getOrderLineSumName, doDeliveryFunctionName)]
#----------------------------------------------------------------------------
# Hydrocache TPC-C Driver
#
# Requires Cloudburst, Anna-Cache (Hydrocache) and Anna
# @author Rafael Soares <joao.rafael.pinto.soares@tecnico.ulisboa.pt>
#----------------------------------------------------------------------------


# ------------------------------------------------------------------------
# Get New Order Index List
#
#   cloudburst - Cloudburst client, required for cloudburst execution
#   write_set - Writeset to be written at the end of transaction
#   warehouse - Transaction warehouse
#   dpw - Districts per warehouse constant
# ------------------------------------------------------------------------

def getNewOrderIndex(cloudburst, write_set, dpw, warehouse):
    new_order_indexes = []
    for d_id in range(1, dpw + 1):
        # Get set of possible new order ids
        new_order_indexes.append(CloudburstReference('NEW_ORDER.INDEXES.GETNEWORDER.%s.%s' % (warehouse, d_id), True))
    return new_order_indexes


# ------------------------------------------------------------------------
# Get New Order List
#
#   cloudburst - Cloudburst client, required for cloudburst execution
#   write_set - Writeset to be written at the end of transaction
#   new_order_indexes - Indexes of each district new order
#   dpw - Districts per warehouse constant
# ------------------------------------------------------------------------

def getNewOrders(cloudburst, write_set, dpw, new_order_indexes):
    no_o_id = []
    for d_id in range(1, dpw + 1):
        cursor = d_id - 1
        if new_order_indexes[cursor] == "None":
            no_o_id.append("None")
        else:
            no_o_id.append(CloudburstReference(new_order_indexes[cursor] + "NO_O_ID", True))
    return no_o_id

# ------------------------------------------------------------------------
# Get Customer ID Query
#
#   cloudburst - Cloudburst client, required for cloudburst execution
#   write_set - Writeset to be written at the end of transaction
#   no_o_id - New Order indexes
#   dpw - Districts per warehouse constant
#   warehouse - Transaction warehouse
# ------------------------------------------------------------------------

def getCustomerID(cloudburst, write_set, dpw, warehouse, no_o_id):
    order_keys = []
    orders_client_id = []
    for d_id in range(1, dpw + 1):
        cursor = d_id - 1
        if no_o_id[cursor] == "None":
            order_keys.append('None')
            orders_client_id.append("None")
        else:
            client_key = 'ORDER.%s.%s.%s' % (warehouse, d_id, no_o_id[cursor])
            order_keys.append(client_key)
            orders_client_id.append(CloudburstReference(client_key + ".O_C_ID", True))
    ol_ids = []

    for d_id in range(1, dpw + 1):
        cursor = d_id - 1
        if no_o_id[cursor] == "None":
            ol_ids.append("None")
        else:
            order_line_key = 'ORDER_LINE.INDEXES.SUMOLAMOUNT.%s.%s.%s' % (no_o_id[cursor], d_id, warehouse)
            ol_ids.append(CloudburstReference(order_line_key, True))

    return no_o_id, order_keys, orders_client_id, ol_ids

# ------------------------------------------------------------------------
# Get Order Line Sum
#
#   cloudburst - Cloudburst client, required for cloudburst execution
#   write_set - Writeset to be written at the end of transaction
#   no_o_id - New Order indexes
#   ordered_keys - Keys of the orders obtained
#   orders_client_id - Client ids of the obtained orders
#   ol_ids - Order Line Ids
#   dpw - Districts per warehouse constant
# ------------------------------------------------------------------------

def getOrderLineSum(cloudburst, write_set, dpw, warehouse, no_o_id, order_keys, orders_client_id, ol_ids):
    sum_order_line = []
    ol_counts = []
    for d_id in range(1, dpw + 1):
        cursor = d_id - 1
        ol_counts.append(0)
        if no_o_id[cursor] == "None":
            sum_order_line.append(0)
        else:
            for order_key in ol_ids[cursor]:
                sum_order_line.append(CloudburstReference(order_key + "OL_AMOUNT", True))
                ol_counts[cursor] += 1

    customer_keys = []
    old_balance_clients = []
    for d_id in range(1, dpw + 1):
        cursor = d_id - 1
        if no_o_id[cursor] == "None":
            customer_keys.append("None")
        else:
            customer_key = "CUSTOMER.%s.%s.%s." % (warehouse, d_id, orders_client_id[cursor])
            customer_keys.append(customer_key)
            old_balance_clients.append(CloudburstReference(customer_key + 'C_BALANCE', True))

    return no_o_id, order_keys, ol_ids, sum_order_line, ol_counts, customer_keys, old_balance_clients

# ------------------------------------------------------------------------
# Execute TPC-C Delivery Transaction
#
# @param dictionary params (transaction parameters)
#	{
#		"w_id"          : value,
#		"o_carrier_id"  : value,
#		"ol_delivery_d" : value,
#	}
#   dpw - districts per warehouse constant
#   no_o_id - New Order indexes
#   ordered_keys - Keys of the orders obtained
#   orders_client_id - Client ids of the obtained orders
#   ol_ids - Order Line Ids
# ------------------------------------------------------------------------
def doDeliveryFunction(cloudburst, write_set, params, dpw, new_order_ids, no_o_id, order_keys, ol_ids, sum_order_line,
                       ol_counts, customer_keys, old_balance_clients):
    # Initialize input parameters
    w_id = params["w_id"]
    o_carrier_id = params["o_carrier_id"]
    ol_delivery_d = params["ol_delivery_d"]
    # Initialize result set
    result = []

    # -------------------------
    # Initialize Data Holders
    # -------------------------
    ol_total = []
    customer_key = []
    for d_id in range(1, dpw + 1):
        ol_total.append(0)
        customer_key.append(None)

    index = 0
    counter = 0

    for ol_amount in sum_order_line:
        counter += 1
        if counter > ol_counts[index]:
            index += 1
            counter = 0
        elif ol_amount != 0:
            ol_total[index] += float(ol_amount)

    for d_id in range(1, dpw + 1):
        cursor = d_id - 1
        if no_o_id[cursor] == "None":
            ## No orders for this district: skip it.
            ## Note: This must be reported if > 1%
            continue

        # ------------------------
        # Delete New Order Query
        # ------------------------

        # Delete New_Order
        new_order_key = 'NEW_ORDER.%s.%s.%s.' % (w_id, d_id, no_o_id[cursor])
        cloudburst.write(write_set, new_order_key + "NO_O_ID", "None")
        cloudburst.write(write_set, new_order_key + "NO_D_ID", "None")
        cloudburst.write(write_set, new_order_key + "NO_W_ID", "None")

        # Remove from NEW_ORDER.IDS
        new_order_ids.remove(new_order_key)
        cloudburst.write(write_set, 'NEW_ORDER.IDS', new_order_ids)

        # Remove new_order index
        new_order_index_key = 'NEW_ORDER.INDEXES.GETNEWORDER.%s.%s' % (w_id, d_id)
        cloudburst.write(write_set, new_order_index_key, "None")

        # ---------------------
        # Update Orders Query
        # ---------------------
        order_key = order_keys[cursor] + '.W_CARRIER_ID'
        cloudburst.write(write_set, order_key, o_carrier_id)

        # -------------------------
        # Update Order Line Query
        # -------------------------
        for order_line in ol_ids[cursor]:
            cloudburst.write(write_set, order_line + 'OL_DELIVERY_D', ol_delivery_d)

    # -----------------------
    # Update Customer Query
    # -----------------------

    for d_id in range(1, dpw + 1):
        cursor = d_id - 1
        if no_o_id[cursor] == "None":
            continue
        else:
            new_balance = float(old_balance_clients[cursor]) + float(ol_total[cursor])
            cloudburst.write(write_set, customer_keys[cursor] + 'C_BALANCE', new_balance)
            result.append((d_id, no_o_id[cursor]))

    return result
# End doDelivery()

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


def doNewOrderFunction(cloudburst, write_set, params, items, all_local, w_tax, d_tax, d_next_o_id, customer_info,
                       c_discount, order_search_index, stocks, constant_null_carrier_id, constant_original_string):
    t0 = time.time()
    w_id = params["w_id"]
    d_id = params["d_id"]
    c_id = params["c_id"]
    o_entry_d = params["o_entry_d"]
    i_ids = params["i_ids"]
    i_w_ids = params["i_w_ids"]
    i_qtys = params["i_qtys"]

    # Adds Client discount to customer info (in order to avoid fetching it twice on references)
    customer_info.append(c_discount)

    # -------------------------------
    # Increment Next Order ID Query
    # -------------------------------
    district_key = "DISTRICT.%s.%s." % (w_id, d_id)
    district_next_order_id_key = district_key + 'D_NEXT_O_ID'
    cloudburst.write(write_set, district_next_order_id_key, d_next_o_id + 1)

    # --------------------
    # Create Order Query
    # --------------------
    order_key = "ORDER.%s.%s.%s." % (w_id, d_id, d_next_o_id)
    ol_cnt = len(i_ids)
    cloudburst.write(write_set, order_key + "O_ID", d_next_o_id)
    cloudburst.write(write_set, order_key + "O_D_ID", d_id)
    cloudburst.write(write_set, order_key + "O_W_ID", w_id)
    cloudburst.write(write_set, order_key + "O_C_ID", c_id)
    cloudburst.write(write_set, order_key + "O_C_ID", c_id)
    cloudburst.write(write_set, order_key + "O_ENTRY_D", o_entry_d)
    cloudburst.write(write_set, order_key + "O_CARRIER_ID", constant_null_carrier_id)
    cloudburst.write(write_set, order_key + "O_OL_CNT", ol_cnt)
    cloudburst.write(write_set, order_key + "O_ALL_LOCAL", all_local)

    order_search_index.append(order_key)
    cloudburst.write(write_set, 'ORDERS.INDEXES.ORDERSEARCH.%s.%s.%s' % (w_id, d_id, c_id), order_search_index)

    # ------------------------
    # Create New Order Query
    # ------------------------
    new_order_key = "NEW_ORDER.%s.%s.%s." % (w_id, d_id, d_next_o_id)
    cloudburst.write(write_set, new_order_key + "NO_O_ID", d_next_o_id)
    cloudburst.write(write_set, new_order_key + "NO_D_ID", d_id)
    cloudburst.write(write_set, new_order_key + "NO_W_ID", w_id)
    cloudburst.write(write_set, 'NEW_ORDER.INDEXES.GETNEWORDER.%s.%s' % (w_id, d_id), new_order_key)

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
    order_line_keys = []
    for i in range(len(i_ids)):
        ol_number.append(i + 1)
        ol_supply_w_id.append(i_w_ids[i])
        ol_i_id.append(i_ids[i])
        ol_quantity.append(i_qtys[i])

        i_price.append(float(items[i * 3]))
        i_name.append(items[i * 3 + 1])
        i_data.append(items[i * 3 + 2])
        stock_key.append("%s.%s." % (ol_supply_w_id[i], ol_i_id[i]))

    # We divide by 6 since for each stock we obtained 6 keys
    for i in range(len(stocks) // 6):
        s_quantity = float(stocks[i * 6])
        s_ytd = float(stocks[i * 6 + 1])
        s_order_cnt = float(stocks[i * 6 + 2])
        s_remote_cnt = float(stocks[i * 6 + 3])
        s_data = stocks[i * 6 + 4]
        s_dist_xx = stocks[i * 6 + 5]

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

        if i_data[i].find(constant_original_string) != -1 and s_data.find(constant_original_string) != -1:
            brand_generic = 'B'
        else:
            brand_generic = 'G'

        ## Transaction profile states to use "ol_quantity * i_price"
        ol_amount = ol_quantity[i] * i_price[i]
        total += ol_amount

        # -------------------------
        # Create Order Line Query
        # -------------------------
        order_line_key = "ORDER_LINE.%s.%s.%s.%s." % (w_id, d_id, d_next_o_id, ol_number[i])

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
        order_line_keys.append(order_line_key)

        item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))
    ## End for i in range(len(stocks) // 6):

    cloudburst.write(write_set, 'ORDER_LINE.INDEXES.SUMOLAMOUNT.%s.%s.%s' % (d_next_o_id, d_id, w_id),
                     order_line_keys)
    ## Adjust the total for the discount
    total *= (1 - c_discount) * (1 + w_tax + d_tax)

    ## Pack up values the client is missing (see TPC-C 2.4.3.5)
    misc = [(w_tax, d_tax, d_next_o_id, total)]
    print("Time: " + str((time.time() - t0)))
    return [customer_info, misc, item_data]
# End doNewOrderDag


value_output = 1
real_output_key = 'output_key'
dag_name = "testFunctionDag"
serializer = Serializer()

print("Starting Cloudburst Connection")
cloudburst = CloudburstConnection(elb_address, elb_address, local=local)

# Register output key
lattice = serializer.dump_lattice(value_output, MultiKeyCausalLattice)
print("Inserting key " + real_output_key)
cloudburst.kvs_client.put(real_output_key, lattice)

# Register doNewOrder functions
cloudburst.register(doNewOrderFunction, doNewOrderFunctionName)

# Register doDelivery functions
cloudburst.register(getNewOrderIndex, getNewOrderIndexName)
cloudburst.register(getNewOrders, getNewOrdersName)
cloudburst.register(getCustomerID, getCustomerIDName)
cloudburst.register(getOrderLineSum, getOrderLineSumName)
cloudburst.register(doDeliveryFunction, doDeliveryFunctionName)

print("Registering dag")
# Register doNewOrderDag
success, error = cloudburst.register_dag(doNewOrderDagName, functions, connections)
print("Registered doNewOrderDag")

# Register doDeliveryDag
success, error = cloudburst.register_dag(doDeliveryDagName, doDeliveryFunctions, doDeliveryConnections)
print("Registered doDeliveryDag")

print("Registered dags")
