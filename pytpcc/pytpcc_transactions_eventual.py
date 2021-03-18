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
import uuid

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

# doOrderStatus
doOrderStatusClientDagName = 'doOrderStatusClientDag'
doOrderStatusClientIndexDagName = 'doOrderStatusClientIndexDagName'

getClientByLastNameFunctionName = 'getClientByLastName'
getClientByFirstNameFunctionName = 'getClientByFirstName'
getLastOrderName = 'getLastOrder'
getOrdersName = 'getOrders'
getOrderLinesIndexesName = 'getOrderLinesIndexes'
getOrderLinesName = 'getOrderLines'
doOrderStatusFunctionName = 'doOrderStatusFunction'

doOrderStatusClientFunctions = [getLastOrderName, getOrdersName, getOrderLinesIndexesName,
                                getOrderLinesName, doOrderStatusFunctionName]
doOrderStatusClientConnections = [(getLastOrderName, getOrdersName), (getOrdersName, getOrderLinesIndexesName),
                                  (getOrderLinesIndexesName, getOrderLinesName),
                                  (getOrderLinesName, doOrderStatusFunctionName)]

doOrderStatusClientIndexFunctions = [getClientByLastNameFunctionName, getClientByFirstNameFunctionName,
                                     getLastOrderName, getOrdersName, getOrderLinesIndexesName,
                                     getOrderLinesName, doOrderStatusFunctionName]

doOrderStatusClientIndexConnections = [(getClientByLastNameFunctionName, getClientByFirstNameFunctionName),
                                       (getClientByFirstNameFunctionName, getLastOrderName),
                                       (getLastOrderName, getOrdersName), (getOrdersName, getOrderLinesIndexesName),
                                       (getOrderLinesIndexesName, getOrderLinesName),
                                       (getOrderLinesName, doOrderStatusFunctionName)]

# doPayment
doPaymentClientDagName = 'doPaymentClientDag'
doPaymentClientIndexDagName = 'doPaymentClientIndexDag'

getClientByLastNameDoPaymentName = 'getClientByLastNameDoPayment'
getClientByFirstNameDoPaymentName = 'getClientByFirstNameDoPayment'
getWarehouseDistrictName = 'getWarehouseDistrict'
doPaymentFunctionName = 'doPaymentFunction'

doPaymentClientFunctions = [getWarehouseDistrictName, doPaymentFunctionName]
doPaymentClientConnections = [(getWarehouseDistrictName, doPaymentFunctionName)]

doPaymentClientIndexFunctions = [getClientByLastNameDoPaymentName, getClientByFirstNameDoPaymentName,
                                 getWarehouseDistrictName, doPaymentFunctionName]
doPaymentClientIndexConnections = [(getClientByLastNameDoPaymentName, getClientByFirstNameDoPaymentName),
                                   (getClientByFirstNameDoPaymentName, getWarehouseDistrictName),
                                   (getWarehouseDistrictName, doPaymentFunctionName)]

# doStockLevel
doStockLevelDagName = 'doStockLevelDag'

getOrderIDName = 'getOrderID'
getStockCountName = 'getStockCount'
getOrderLinesStockLevelName = 'getOrderLinesStockLevel'
getStocksName = 'getStocks'
doStockLevelFunctionName = 'doStockLevelFunction'

doStockLevelFunctions = [getOrderIDName, getStockCountName, getOrderLinesStockLevelName,
                         getStocksName, doStockLevelFunctionName]

doStockLevelConnections = [(getOrderIDName, getStockCountName), (getStockCountName, getOrderLinesStockLevelName),
                           (getOrderLinesStockLevelName, getStocksName), (getStocksName, doStockLevelFunctionName)]

#----------------------------------------------------------------------------
# Hydrocache TPC-C Driver
#
# Requires Cloudburst, Anna-Cache (Hydrocache) and Anna
# @author Rafael Soares <joao.rafael.pinto.soares@tecnico.ulisboa.pt>
#----------------------------------------------------------------------------

#----------------------------------------------------------------------------
# DoDelivery Transaction
#----------------------------------------------------------------------------

# ------------------------------------------------------------------------
# Get New Order Index List
#
#   cloudburst - Cloudburst client, required for cloudburst execution
#   write_set - Writeset to be written at the end of transaction
#   warehouse - Transaction warehouse
#   dpw - Districts per warehouse constant
# ------------------------------------------------------------------------

def getNewOrderIndex(cloudburst, dpw, warehouse):
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

def getNewOrders(cloudburst, dpw, new_order_indexes):
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

def getCustomerID(cloudburst, dpw, warehouse, no_o_id):
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

def getOrderLineSum(cloudburst, dpw, warehouse, no_o_id, order_keys, orders_client_id, ol_ids):
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
            old_balance_clients.append("None")
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
def doDeliveryFunction(cloudburst, params, dpw, no_o_id, order_keys, ol_ids, sum_order_line,
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
        cloudburst.put(new_order_key + "NO_O_ID", "None")
        cloudburst.put(new_order_key + "NO_D_ID", "None")
        cloudburst.put(new_order_key + "NO_W_ID", "None")

        # Remove new_order index
        new_order_index_key = 'NEW_ORDER.INDEXES.GETNEWORDER.%s.%s' % (w_id, d_id)
        cloudburst.put(new_order_index_key, "None")

        # ---------------------
        # Update Orders Query
        # ---------------------
        order_key = order_keys[cursor] + '.W_CARRIER_ID'
        cloudburst.put(order_key, o_carrier_id)

        # -------------------------
        # Update Order Line Query
        # -------------------------
        for order_line in ol_ids[cursor]:
            cloudburst.put(order_line + 'OL_DELIVERY_D', ol_delivery_d)

    # -----------------------
    # Update Customer Query
    # -----------------------

    for d_id in range(1, dpw + 1):
        cursor = d_id - 1
        if no_o_id[cursor] == "None":
            continue
        else:
            new_balance = float(old_balance_clients[cursor]) + float(ol_total[cursor])
            cloudburst.put(customer_keys[cursor] + 'C_BALANCE', new_balance)
            result.append((d_id, no_o_id[cursor]))

    return result
# End doDelivery()



#----------------------------------------------------------------------------
# DoNew Order Transaction
#----------------------------------------------------------------------------

# ------------------------------------------------------------------------
# doNewOrder transaction to be registered and executed by cloudburst
# ------------------------------------------------------------------------

def doNewOrderFunction(cloudburst, params, items, all_local, w_tax, d_tax, d_next_o_id, customer_info,
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
    cloudburst.put(district_next_order_id_key, d_next_o_id + 1)

    # --------------------
    # Create Order Query
    # --------------------
    order_key = "ORDER.%s.%s.%s." % (w_id, d_id, d_next_o_id)
    ol_cnt = len(i_ids)
    cloudburst.put(order_key + "O_ID", d_next_o_id)
    cloudburst.put(order_key + "O_D_ID", d_id)
    cloudburst.put(order_key + "O_W_ID", w_id)
    cloudburst.put(order_key + "O_C_ID", c_id)
    cloudburst.put(order_key + "O_C_ID", c_id)
    cloudburst.put(order_key + "O_ENTRY_D", o_entry_d)
    cloudburst.put(order_key + "O_CARRIER_ID", constant_null_carrier_id)
    cloudburst.put(order_key + "O_OL_CNT", ol_cnt)
    cloudburst.put(order_key + "O_ALL_LOCAL", all_local)

    order_search_index.append(order_key)
    cloudburst.put('ORDERS.INDEXES.ORDERSEARCH.%s.%s.%s' % (w_id, d_id, c_id), order_search_index)

    # ------------------------
    # Create New Order Query
    # ------------------------
    new_order_key = "NEW_ORDER.%s.%s.%s." % (w_id, d_id, d_next_o_id)
    cloudburst.put(new_order_key + "NO_O_ID", d_next_o_id)
    cloudburst.put(new_order_key + "NO_D_ID", d_id)
    cloudburst.put(new_order_key + "NO_W_ID", w_id)
    cloudburst.put('NEW_ORDER.INDEXES.GETNEWORDER.%s.%s' % (w_id, d_id), new_order_key)

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

        cloudburst.put(current_stock_key + "S_QUANTITY", s_quantity)
        cloudburst.put(current_stock_key + "S_YTD", s_ytd)
        cloudburst.put(current_stock_key + "S_ORDER_CNT", s_order_cnt)
        cloudburst.put(current_stock_key + "S_REMOTE_CNT", s_remote_cnt)

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

        cloudburst.put(order_line_key + "OL_O_ID", d_next_o_id)
        cloudburst.put(order_line_key + "OL_D_ID", d_id)
        cloudburst.put(order_line_key + "OL_W_ID", w_id)
        cloudburst.put(order_line_key + "OL_NUMBER", ol_number[i])
        cloudburst.put(order_line_key + "OL_I_ID", ol_i_id[i])
        cloudburst.put(order_line_key + "OL_SUPPLY_W_ID", ol_supply_w_id[i])
        cloudburst.put(order_line_key + "OL_DELIVERY_D", o_entry_d)
        cloudburst.put(order_line_key + "OL_QUANTITY", ol_quantity[i])
        cloudburst.put(order_line_key + "OL_AMOUNT", ol_amount)
        cloudburst.put(order_line_key + "OL_DISTRICT_INFO", s_dist_xx)
        order_line_keys.append(order_line_key)

        item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))
    ## End for i in range(len(stocks) // 6):

    cloudburst.put('ORDER_LINE.INDEXES.SUMOLAMOUNT.%s.%s.%s' % (d_next_o_id, d_id, w_id),
                     order_line_keys)
    ## Adjust the total for the discount
    total *= (1 - c_discount) * (1 + w_tax + d_tax)

    ## Pack up values the client is missing (see TPC-C 2.4.3.5)
    misc = [(w_tax, d_tax, d_next_o_id, total)]
    print("Time: " + str((time.time() - t0)))
    return [customer_info, misc, item_data]
# End doNewOrderDag

#----------------------------------------------------------------------------
# Do Order Status Transaction
#----------------------------------------------------------------------------

def getClientByLastName(cloudburst, customers):
    customer_first_names = []
    for customer_id in customers:
        customer_first_names.append(CloudburstReference(customer_id + "C_FIRST", True))

    return customers, customer_first_names

def getClientByFirstName(cloudburst, customer_ids, customer_first_names):
    customer_ids_order = []
    customers_names_order = []

    customer_ids_order.append(customer_ids.pop())
    customers_names_order.append(customer_first_names.pop())

    for count, cust in enumerate(customer_first_names):
        for index in range(len(customers_names_order)):
            if cust < customers_names_order[index]:
                customers_names_order.insert(index, cust)
                customer_ids_order.insert(index, customer_ids[count])
                continue

    assert len(customers_names_order) > 0

    namecnt = len(customers_names_order)
    index = int((namecnt - 1) / 2)
    # Might come float here, check correctness
    customer_key = customer_ids_order[index]
    customer = []
    customer.append(CloudburstReference(customer_key + "C_ID", True))
    customer.append(CloudburstReference(customer_key + "C_D_ID", True))
    customer.append(CloudburstReference(customer_key + "C_W_ID", True))
    customer.append(CloudburstReference(customer_key + "C_FIRST", True))
    customer.append(CloudburstReference(customer_key + "C_MIDDLE", True))
    customer.append(CloudburstReference(customer_key + "C_LAST", True))
    customer.append(CloudburstReference(customer_key + "C_STREET_1", True))
    customer.append(CloudburstReference(customer_key + "C_STREET_2", True))
    customer.append(CloudburstReference(customer_key + "C_CITY", True))
    customer.append(CloudburstReference(customer_key + "C_ZIP", True))
    customer.append(CloudburstReference(customer_key + "C_PHONE", True))
    customer.append(CloudburstReference(customer_key + "C_SINCE", True))
    customer.append(CloudburstReference(customer_key + "C_CREDIT", True))
    customer.append(CloudburstReference(customer_key + "C_CREDIT_LIM", True))
    customer.append(CloudburstReference(customer_key + "C_DISCOUNT", True))
    customer.append(CloudburstReference(customer_key + "C_BALANCE", True))
    customer.append(CloudburstReference(customer_key + "C_YTD_PAYMENT", True))
    customer.append(CloudburstReference(customer_key + "C_PAYMENT_CNT", True))
    customer.append(CloudburstReference(customer_key + "C_DELIVERY_CNT", True))
    customer.append(CloudburstReference(customer_key + "C_DATA", True))

    return customer

def getLastOrder(cloudburst, params, client):
    w_id = params["w_id"]
    d_id = params["d_id"]
    c_id = client[0]

    order_search_key = 'ORDERS.INDEXES.ORDERSEARCH.%s.%s.%s' % (w_id, d_id, c_id)

    return client, CloudburstReference(order_search_key, True)

def getOrders(cloudburst, client, order_search):
    orders = []
    for order in order_search:
        orders.append(CloudburstReference(order + 'O_ID', True))

    return client, order_search, orders

def getOrderLinesIndexes(cloudburst, params, client, order_search, orders):
    w_id = params["w_id"]
    d_id = params["d_id"]
    last_order_oid = 0
    last_order = None

    for index, order in enumerate(orders):
        if order > last_order_oid:
            last_order_oid = order
            last_order = order_search[index]

    order = []
    order_line_index = "None"
    if last_order_oid > 0:
        order = [CloudburstReference(last_order + 'O_ID', True),
                 CloudburstReference(last_order + 'O_CARRIER_ID', True),
                 CloudburstReference(last_order + 'O_ENTRY_D', True)]

        order_line_key = 'ORDER_LINE.INDEXES.SUMOLAMOUNT.%s.%s.%s' % (last_order_oid, d_id, w_id)
        order_line_index = CloudburstReference(order_line_key, True)

    return client, order, order_line_index

def getOrderLines(cloudburst, client, order, order_line_index):
    order_lines = []
    if order_line_index == "None":
        return client, [], []
    else:
        for order_line in order_line_index:
            order_lines.append([CloudburstReference(order_line + 'OL_SUPPLY_W_ID', True),
                                CloudburstReference(order_line + 'OL_I_ID', True),
                                CloudburstReference(order_line + 'OL_QUANTITY', True),
                                CloudburstReference(order_line + 'OL_AMOUNT', True),
                                CloudburstReference(order_line + 'OL_DELIVERY_D', True)])

        return client, order, order_lines

def doOrderStatusFunction(cloudburst, client, order, order_lines):
    return [client, order, order_lines]

#----------------------------------------------------------------------------
# doPayment Transaction
#----------------------------------------------------------------------------

def getClientByLastNameDoPayment(cloudburst, customers):
    customer_ids_first_name = []
    for customer_id in customers:
        customer_ids_first_name.append([customer_id, CloudburstReference(customer_id + "C_FIRST", True)])

    return customer_ids_first_name

def getClientByFirstNameDoPayment(cloudburst, customer_ids_first_name):
    customers = []
    customers.append(customer_ids_first_name.pop())

    for cust in customer_ids_first_name:
        for index in range(len(customers)):
            if cust[1] < customers[index][1]:
                customers.insert(index, cust)
                continue

    assert len(customers) > 0

    namecnt = len(customers)
    index = int((namecnt - 1) / 2)
    customer_key = customers[index][0]
    customer = []
    customer.append(CloudburstReference(customer_key + "C_ID", True))
    customer.append(CloudburstReference(customer_key + "C_D_ID", True))
    customer.append(CloudburstReference(customer_key + "C_W_ID", True))
    customer.append(CloudburstReference(customer_key + "C_FIRST", True))
    customer.append(CloudburstReference(customer_key + "C_MIDDLE", True))
    customer.append(CloudburstReference(customer_key + "C_LAST", True))
    customer.append(CloudburstReference(customer_key + "C_STREET_1", True))
    customer.append(CloudburstReference(customer_key + "C_STREET_2", True))
    customer.append(CloudburstReference(customer_key + "C_CITY", True))
    customer.append(CloudburstReference(customer_key + "C_ZIP", True))
    customer.append(CloudburstReference(customer_key + "C_PHONE", True))
    customer.append(CloudburstReference(customer_key + "C_SINCE", True))
    customer.append(CloudburstReference(customer_key + "C_CREDIT", True))
    customer.append(CloudburstReference(customer_key + "C_CREDIT_LIM", True))
    customer.append(CloudburstReference(customer_key + "C_DISCOUNT", True))
    customer.append(CloudburstReference(customer_key + "C_BALANCE", True))
    customer.append(CloudburstReference(customer_key + "C_YTD_PAYMENT", True))
    customer.append(CloudburstReference(customer_key + "C_PAYMENT_CNT", True))
    customer.append(CloudburstReference(customer_key + "C_DELIVERY_CNT", True))
    customer.append(CloudburstReference(customer_key + "C_DATA", True))

    return customer

def getWarehouseDistrict(cloudburst, params, client):
    w_id = params["w_id"]
    d_id = params["d_id"]

    warehouse_key = 'WAREHOUSE.%s.' % w_id

    warehouse = [CloudburstReference(warehouse_key + 'W_ID', True),
                 CloudburstReference(warehouse_key + 'W_NAME', True),
                 CloudburstReference(warehouse_key + 'W_STREET_1', True),
                 CloudburstReference(warehouse_key + 'W_STREET_2', True),
                 CloudburstReference(warehouse_key + 'W_CITY', True),
                 CloudburstReference(warehouse_key + 'W_STATE', True),
                 CloudburstReference(warehouse_key + 'W_ZIP', True),
                 CloudburstReference(warehouse_key + 'W_YTD', True)]

    district_key = 'DISTRICT.%s.%s.' % (w_id, d_id)

    district = [CloudburstReference(district_key + "D_ID", True),
                CloudburstReference(district_key + "D_W_ID", True),
                CloudburstReference(district_key + "D_NAME", True),
                CloudburstReference(district_key + "D_STREET_1", True),
                CloudburstReference(district_key + "D_STREET_2", True),
                CloudburstReference(district_key + "D_CITY", True),
                CloudburstReference(district_key + "D_STATE", True),
                CloudburstReference(district_key + "D_ZIP", True),
                CloudburstReference(district_key + "D_TAX", True),
                CloudburstReference(district_key + "D_YTD", True),
                CloudburstReference(district_key + "D_NEXT_O_ID", True)]

    return warehouse, district, client

def doPaymentFunction(cloudburst, params, constant_bad_credit, constant_max_c_data,
                      warehouse, district, customer):
    # Initialize transaction properties
    w_id = params["w_id"]
    d_id = params["d_id"]
    h_amount = params["h_amount"]
    c_w_id = params["c_w_id"]
    c_d_id = params["c_d_id"]
    c_id = customer[0]
    c_last = params["c_last"]
    h_date = params["h_date"]

    # Values taken from the getClient function and order
    c_balance = float(customer[15]) - h_amount
    c_ytd_payment = float(customer[16]) + h_amount
    c_payment_cnt = float(customer[17]) + 1
    c_data = customer[19]

    c_credit = customer[12]
    customer_key = 'CUSTOMER.%s.%s.%s.' % (w_id, d_id, c_id)
    if c_credit == constant_bad_credit:
        newData = " ".join(
            map(
                str,
                [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]
            )
        )

        c_data = (newData + "|" + c_data)
        if len(c_data) > constant_max_c_data:
            c_data = c_data[:constant_max_c_data]

        cloudburst.put(customer_key + 'C_BALANCE', c_balance)
        cloudburst.put(customer_key + 'C_YTD_PAYMENT', c_ytd_payment)
        cloudburst.put(customer_key + 'C_PAYMENT_CNT', c_payment_cnt)
        cloudburst.put(customer_key + 'C_DATA', c_data)

    else:
        cloudburst.put(customer_key + 'C_BALANCE', c_balance)
        cloudburst.put(customer_key + 'C_YTD_PAYMENT', c_ytd_payment)
        cloudburst.put(customer_key + 'C_PAYMENT_CNT', c_payment_cnt)
        cloudburst.put(customer_key + 'C_DATA', '')

    h_data = "%s    %s" % (warehouse[1], district[2])

    history_key = 'HISTORY.%s.' % str(uuid.uuid1())

    cloudburst.put(history_key + 'H_C_ID', c_id)
    cloudburst.put(history_key + 'H_C_D_ID', c_d_id)
    cloudburst.put(history_key + 'H_C_W_ID', c_w_id)
    cloudburst.put(history_key + 'H_D_ID', d_id)
    cloudburst.put(history_key + 'H_W_ID', w_id)
    cloudburst.put(history_key + 'H_DATE', h_date)
    cloudburst.put(history_key + 'H_AMOUNT', h_amount)
    cloudburst.put(history_key + 'H_DATA', h_data)

    return [warehouse, district, customer]



#----------------------------------------------------------------------------
# doStockLevel Transaction
#----------------------------------------------------------------------------

def getOrderID(cloudburst, params):
    # Initialize transaction parameters
    w_id = params["w_id"]
    d_id = params["d_id"]

    district_key = 'DISTRICT.%s.%s.' % (w_id, d_id)

    return CloudburstReference(district_key + 'D_NEXT_O_ID', True)

def getStockCount(cloudburst, params, next_o_id):
    w_id = params["w_id"]
    d_id = params["d_id"]
    order_lines = []
    for o_id in range(next_o_id-20, next_o_id):
        order_line_key = 'ORDER_LINE.INDEXES.SUMOLAMOUNT.%s.%s.%s' % (o_id, d_id, w_id)
        order_lines.append(CloudburstReference(order_line_key, True))
    return order_lines

def getOrderLinesStockLevel(cloudburst, order_lines_index):
    items = []
    for order_lines in order_lines_index:
        for order_line in order_lines:
            items.append(CloudburstReference(order_line + 'OL_I_ID', True))

    return items

def getStocks(cloudburst, params, items):
    w_id = params["w_id"]
    unique_items = []
    [unique_items.append(x) for x in items if x not in unique_items]

    stocks = []
    for item in unique_items:
        stock_key = 'STOCK.%s.%s.' % (w_id, item)
        stocks.append(CloudburstReference(stock_key + 'S_QUANTITY', True))

    return stocks

def doStockLevelFunction(cloudburst, params, stocks):
    threshold = params["threshold"]
    stock_counts = {}
    stock_count = 0
    for stock_quantity in stocks:
        if stock_quantity < threshold:
            stock_count += 1

    return stock_count


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

# Register doOrderStatus
cloudburst.register(getClientByLastName, getClientByLastNameFunctionName)
cloudburst.register(getClientByFirstName, getClientByFirstNameFunctionName)
cloudburst.register(getLastOrder, getLastOrderName)
cloudburst.register(getOrders, getOrdersName)
cloudburst.register(getOrderLinesIndexes, getOrderLinesIndexesName)
cloudburst.register(getOrderLines, getOrderLinesName)
cloudburst.register(doOrderStatusFunction, doOrderStatusFunctionName)

# Register doPayment functions
cloudburst.register(getClientByLastNameDoPayment, getClientByLastNameDoPaymentName)
cloudburst.register(getClientByFirstNameDoPayment, getClientByFirstNameDoPaymentName)
cloudburst.register(getWarehouseDistrict, getWarehouseDistrictName)
cloudburst.register(doPaymentFunction, doPaymentFunctionName)

# Register doStockLevel functions
cloudburst.register(getOrderID, getOrderIDName)
cloudburst.register(getStockCount, getStockCountName)
cloudburst.register(getOrderLinesStockLevel, getOrderLinesStockLevelName)
cloudburst.register(getStocks, getStocksName)
cloudburst.register(doStockLevelFunction, doStockLevelFunctionName)

print("Registering dag")
# Register doNewOrderDag
success, error = cloudburst.register_dag(doNewOrderDagName, functions, connections)
print("Registered doNewOrderDag")

# Register doDeliveryDag
success, error = cloudburst.register_dag(doDeliveryDagName, doDeliveryFunctions, doDeliveryConnections)
print("Registered doDeliveryDag")

# Register doOrderStatusClientDag
success, error = cloudburst.register_dag(doOrderStatusClientDagName, doOrderStatusClientFunctions,
                                         doOrderStatusClientConnections)

# Register doOrderStatusClientDag
success, error = cloudburst.register_dag(doOrderStatusClientIndexDagName, doOrderStatusClientIndexFunctions,
                                         doOrderStatusClientIndexConnections)
print("Registered doOrderStatusDags")

# Register doPayment
success, error = cloudburst.register_dag(doPaymentClientDagName, doPaymentClientFunctions,
                                         doPaymentClientConnections)

success, error = cloudburst.register_dag(doPaymentClientIndexDagName, doPaymentClientIndexFunctions,
                                         doPaymentClientIndexConnections)

# Register doStockLevelDag
success, error = cloudburst.register_dag(doStockLevelDagName, doStockLevelFunctions,
                                         doStockLevelConnections)
print("Registered doStockLevelDag")

print("Registered dags")
