import asyncio
import concurrent
import threading
import logging
import json
import ledgerx

import datetime as dt

from ledgerx.util import unique_values_from_key

class MarketState:

    # state of market and positions
    all_contracts = dict()
    traded_contract_ids = dict()
    expired_contracts = dict()
    contract_positions = dict() # my positions by contract (no lots) dict(contract_id: position)
    accounts = dict()           # dict(asset: dict(available_balance: 0, position_locked_amount: 0, ...))
    exp_dates = list()
    exp_strikes = dict()        # dict(exp_date : dict(asset: [sorted list of strike prices (int)]))
    orders = dict()             # all orders in the market dict{contract_id: dict{mid: order}}
    book_states = dict()        # all books in the market  dict{contract_id : dict{mid : book_state}}
    book_top = dict()           # all top books in the market dict{contract_id : top}
    to_update_basis = dict()    # dict(contract_id: position)
    label_to_contract_id = dict() # dict(contract['label']: contract_id)
    put_call_map = dict()         # dict(contract_id: contract_id) put -> call and call -> put
    costs_to_close = dict()       # dict(contract_id: dict(net, cost, basis, size, bid, ask, low, high))
    last_heartbeat = None
    mpid = None
    cid = None
    next_day_contracts = dict()
    skip_expired = True

    # Constants
    risk_free = 0.005 # 0.5% risk free interest
    timezone = dt.timezone.utc
    strptime_format = "%Y-%m-%d %H:%M:%S%z"
    seconds_per_year = 3600.0 * 24.0 * 365.0  # ignore leap year, okay?

    def __init__(self, skip_expired : bool = True):
        self.clear()
        self.skip_expired = skip_expired

    def clear(self):
        self.all_contracts = dict()
        self.traded_contract_ids = dict()
        self.expired_contracts = dict()
        self.contract_positions = dict()
        self.accounts = dict()
        self.exp_dates = list()
        self.exp_strikes = dict()
        self.orders = dict()
        self.book_states = dict()
        self.book_top = dict()
        self.to_update_basis = dict()
        self.label_to_contract_id = dict()
        self.put_call_map = dict()
        self.costs_to_close = dict()
        self.next_day_contracts = dict()
        self.skip_expired = True
        # keep any previous heartbeats, mpid and cid

    def mid(self, bid, ask):
        if bid is None and ask is None:
            return None
        elif bid is not None:
            if ask is not None:
                return (bid + ask) /2
            else:
                return bid
        else:
            return ask
    
    def cost_to_close(self, contract_id):
        "returns dict(low, high, net, basis, cost, ask, bid, size)"
        logging.debug(f"getting cost to close for {contract_id}")
        
        if contract_id not in self.contract_positions:
            return None
        contract = self.all_contracts[contract_id]
        if self.contract_is_expired(contract):
            return None
        position = self.contract_positions[contract_id]
        size = position['size']
        if size == 0:
            return None
        if contract_id not in self.book_top:
            logging.info(f"Need books for {contract_id}")
            self.load_books(contract_id)
        if contract_id not in self.book_top:
            logging.warn(f"There are no books for {contract_id} {contract}")
            return None
        
        top = self.book_top[contract_id]
        bid = self.bid(top)
        ask = self.ask(top)
        mid = self.mid(bid, ask)
        fee = None
        cost = None
        if mid is not None:
            fee = self.fee(mid, size)
            cost = (fee + mid * size) // 10000
        basis = None
        net = None
        if 'basis' in position:
            basis = position['basis'] // 100
            if size < 0 and ask is not None:
                net = int((fee + ask * size) // 10000 - basis)
            elif bid is not None:
                net = int((fee + bid * size) // 10000 - basis)
        if basis is not None:
            if contract_id not in self.costs_to_close or cost != self.costs_to_close[contract_id]['cost']:
                logging.info(f"net ${net}: cost ${cost} - basis ${basis} to close {size} of {self.all_contracts[contract_id]['label']} at {bid} to {ask}")
        else:
            if contract_id in self.contract_positions:
                self.to_update_basis[contract_id] = self.contract_positions[contract_id]
            logging.warn(f"No basis for ${cost} to close {size} of {self.all_contracts[contract_id]['label']} at {bid} to {ask}")
        low = None
        high = None
        if size < 0:
            if bid is not None:
                low = (fee + bid * size) // 10000
            if ask is not None:
                high = (fee + ask * size) // 10000
        else:
            if ask is not None:
                low = (fee + ask * size) // 10000
            if bid is not None:
                high= (fee + bid * size) // 10000
        ret = dict(net=net, cost=cost, basis=basis, size=size, bid=bid, ask=ask, fee=fee, low=low, high=high)
        self.costs_to_close[contract_id] = ret
        return ret

    @classmethod
    def ask(cls, top_book):
        if 'ask' in top_book:
            ask = top_book['ask']
            if ask is not None and ask != 0:
                return ask
        return None

    @classmethod
    def bid(cls, top_book):
        if 'bid' in top_book:
            bid = top_book['bid']
            if bid is not None and bid != 0:
                return bid
        return None

    @classmethod
    def fee(cls, price, size, price_units = 100):
        # $0.15 per contract or 20% of price whichever is less
        fee_per_contract = price // (5 * price_units) # 20%
        if fee_per_contract >= 15:
            fee_per_contract = 15
        return abs(size) * fee_per_contract

    @classmethod
    def is_same_option_date(cls, contract_a, contract_b):
        return 'is_call' in contract_a and 'is_call' in contract_b and \
            contract_a['is_call'] == contract_b['is_call'] and \
            contract_a['date_expires'] == contract_b['date_expires'] and \
            contract_a['derivative_type'] == contract_b['derivative_type'] and \
            contract_a['underlying_asset'] == contract_b['underlying_asset']

    def contract_is_expired(self, contract):
        if 'date_expires' not in contract:
            logging.warn(f"invalid contract without expiration: {contract}")
        exp = dt.datetime.strptime(contract['date_expires'], self.strptime_format)
        if (exp - dt.datetime.now(self.timezone)).total_seconds() < 10: # do not risk any last second trades...
            return True
        else:
            return contract['id'] in self.expired_contracts

    def is_qualified_covered_call(self, contract_id):
        if contract_id not in self.all_contracts:
            self.retrieve_contract(contract_id)
        contract = self.all_contracts[contract_id]
        if contract['is_call'] == False:
            return False
        exp = dt.datetime.strptime(contract['date_expires'], self.strptime_format)
        days = (exp - dt.datetime.now(self.timezone)).total_seconds() / (3600 * 24)
        if days <= 30:
            return False
        
        next_day_contract = self.get_next_day_swap(contract['underlying_asset'])
        next_day_id = None
        if next_day_contract is not None:
            next_day_id = next_day_contract['id']
        if next_day_id is not None and next_day_id in self.book_top:
            top = self.book_top[next_day_id]
            bid = self.bid(top)
            ask = self.ask(top)
            fmv = bid
            if ask is not None:
                if bid is not None:
                    fmv = (bid + ask) / 2
            if fmv is not None:
                # get all strikes for this call option
                strikes = []
                for test_id, test_contract in self.all_contracts.items():
                    if self.is_same_option_date(contract, test_contract):
                        strikes.append(test_contract['strike_price'])
                strikes.sort(reverse = True)
                lowest_strike = strikes[0]
                past_fmv = 0
                for strike in strikes:
                    if strike <= fmv:
                        past_fmv += 1
                    if past_fmv <= 1 and days > 30:
                        lowest_strike = strike
                    if past_fmv <= 2 and days > 90:
                        lowest_strike = strike
                if contract['strike_price'] >= lowest_strike:
                    return True
        return False


    def add_expiration_date(self, date):
        assert(date not in self.exp_dates)
        self.exp_dates.append(date)
        self.exp_dates.sort()

    def is_my_order(self, order):
        return self.mpid is not None and 'mpid' in order and self.mpid == order['mpid']

    def replace_existing_order(self, order):
        # replace if clock is larger
        mid = order['mid']
        contract_id = order['contract_id']
        assert(contract_id in self.orders and mid in self.orders[contract_id])
        contract_orders = self.orders[contract_id]
        existing = contract_orders[mid]
        assert(order['contract_id'] in self.all_contracts)
        contract = self.all_contracts[contract_id]
        label = contract['label']
        if existing['clock'] <= order['clock'] and existing['ticks'] < order['ticks']:
            if self.is_my_order(existing) and not self.is_my_order(order):
                logging.warn("Existing order is mine but replacement is not. existing {existing} order {order}, ignoring update")
            else:
                if order['size'] == 0:
                    del contract_orders[mid]
                    logging.debug(f"Deleted existing order of zero size {existing} to {order}")
                else:
                    contract_orders[mid] = order
                    logging.debug(f"Replaced existing order on {label} {existing} to {order}")
        else:
            if existing['ticks'] == order['ticks']:
                logging.debug(f"Got duplicate order on {label} {existing} vs {order}")
            else:
                logging.warn(f"existing order on {label} {existing} is newer {order}, ignoring update")
    
    def insert_new_order(self, order):
        mid = order['mid']
        contract_id = order['contract_id']
        if contract_id not in self.orders:
            self.orders[contract_id] = dict()
        assert(mid not in self.orders)
        label = self.all_contracts[contract_id]['label']
        if self.is_my_order(order):
            assert(contract_id not in self.orders or mid not in self.orders[contract_id])
            logging.info(f"Inserted my new order on {label} order {order}")
        else:
            logging.debug(f"Inserted this 3rd party order on {label} order {order}")
        self.orders[contract_id][mid] = order

    def handle_order(self, order):
        is_my_order = self.is_my_order(order)
        mid = order['mid']
        contract_id = order['contract_id']

        # update the contract if needed
        if contract_id not in self.all_contracts:
            logging.warn(f"unknown contract {contract_id}... Retrieving it")
            self.retrieve_contract(contract_id)
        contract = self.all_contracts[contract_id]
        label = contract['label']
        
        if contract_id not in self.orders:
            self.orders[contract_id] = dict()
        contract_orders = self.orders[contract_id]

        status = order['status_type']
        exists = mid in contract_orders
        existing = None
        if exists:
            existing = contract_orders[mid]
            if 'mpid' in order and not is_my_order:
                logging.warn(f"different mpid {self.mpid} for mid {mid} existing {existing} order {order}")
        
        if not exists and status != 200:
            logging.debug(f"traded order had not been tracked yet! {order}")
            self.insert_new_order(order)
            exists = True
            existing = contract_orders[mid]
        
        logging.debug(f"handle_order on {contract_id} {label} {order}")
        if status == 200:
            # A resting order was inserted
            if exists:
                self.replace_existing_order(order)
            else:
                self.insert_new_order(order)
            self.handle_book_state(contract_id, order)
        elif status == 201:
            # a cross (trade) occured            
            if is_my_order:
                # This is my traded order, so track position and basis deltas
                if mid in contract_orders:
                    if 'mpid' in contract_orders[mid]:
                        assert(mid in contract_orders and self.mpid == contract_orders[mid]['mpid'])
                    else:
                        logging.warn(f"How can my order not have my mpid? existing {existing} order {order} mid {mid} {contract_orders}")
                delta_pos = order['filled_size']
                delta_basis = order['filled_size'] * order['filled_price']
                
                if order['is_ask']:
                    # sold
                    logging.info(f"Observed sale of {delta_pos} for ${delta_basis//100} on {contract_id} {label} {order}")
                else:
                    # bought
                    logging.info("Observed purchase of {delta_pos} for ${delta_basis//100} on {contract_id} {label} {order}/mi")

                if order['size'] != 0:
                    logging.info(f"Partial fill Cross trade {delta_pos} ${delta_basis//100} {existing} {order}")
                    self.replace_existing_order(order)
                else:
                    logging.info(f"Full fill Cross trade {delta_pos} ${delta_basis//100} {existing} {order}")
                    del contract_orders[mid]

                #if 'id' in position:
                #    size = position['size']
                #    basis = position['basis']
                #    self.update_position(contract_id, position)
                #    if position['size'] != size or position['basis'] != basis:
                #        logging.warn(f"After refresh of trades, size and/or basis do not agree with approximation: {size} {basis} {position} {order}")

            else:
                logging.debug(f"Updating order for books {order}")
                self.replace_existing_order(order)

            if order['size'] != 0:
                self.handle_book_state(contract_id, order)
            else:
                self.delete_book_state(contract_id, mid)

        elif status == 202:
            # A market order was not filled
            logging.warn(f"dunno how to handle not filled market order on {label} {existing} {order}")
        elif status == 203:
            # cancelled
            if exists:
                logging.debug(f"Deleting cancelled order on {label} {existing} {order}")
                del contract_orders[mid]
            else:
                logging.debug(f"Ignoring untracked cancelled order on {label} {order}")
            # handle copy in book_states, if needed
            self.delete_book_state(contract_id, mid)
        elif status == 300:
            logging.info(f"Acknowledged on {label} {existing} {order}")
        elif status == 610:
            # expired
            logging.info(f"Expired on {label} {existing} {order}")
            if exists:
                del contract_orders[mid]
            self.delete_book_state(contract_id, mid)
        elif status >= 600:
            logging.warn(f"invalid or rejected order {order}")
            if exists:
                del contract_orders[mid]

    def get_top_from_book_state(self, contract_id):
        if contract_id not in self.book_states:
            logging.info(f"need books for {contract_id}")
            self.load_books(contract_id)
        books = self.book_states[contract_id]
        ask = None
        bid = None
        if contract_id not in self.all_contracts:
            self.retrieve_contract(contract_id)
        contract = self.all_contracts[contract_id]
        logging.debug(f"get_top_from_book_state contract_id {contract_id} contract {contract} books {books}")
        clock = -1
        for mid,book in books.items():
            assert(mid == book['mid'])
            is_ask = book['is_ask']
            price = book['price']
            if is_ask:
                if ask is None or ask > price:
                    ask = price
            else:
                if bid is None or bid < price:
                    bid = price
            if clock < book['clock']:
                clock = book['clock']
        book_top = dict(ask= ask, bid= bid, contract_id= contract_id, contract_type= None, clock=clock, type= 'book_top')
        if contract_id not in self.book_top or self.book_top[contract_id]['clock'] < clock:
            self.book_top[contract_id] = book_top
        logging.info(f"Top for {contract_id} {contract['label']} {book_top}")
        return book_top
        
    def handle_book_state(self, contract_id, book_state):
        """{clock": 57906, "entry_id": "81d87376167f400fb6545234600856b2", "is_ask": true, "price": 884000, "size": 1}"""
        logging.debug(f"handle_book_state {contract_id} {book_state}")
        assert('mid' in book_state)
        if contract_id not in self.book_states:
            logging.info(f"Ignoring book state for {contract_id} as no books have been loaded or are loading")
            return
        books = self.book_states[contract_id]
        mid = book_state['mid']
        if mid in books:
            book_order = books[mid]
            if book_state['clock'] < book_order['clock']:
                logging.info(f"Ignoring old book_state={book_state} orig={book_order}")
                return
            for key in book_order.keys():
                if key in book_state:
                    book_order[key] = book_state[key]
        else:
            books[mid] = book_state

    def handle_all_book_states(self, book_states):
        assert('contract_id' in book_states)
        assert('book_states' in book_states)
        contract_id = book_states['contract_id']
        if contract_id not in self.all_contracts:
            self.retrieve_contract(contract_id)
        logging.info(f"Loading all books for {contract_id}: {self.all_contracts[contract_id]['label']}")
        # replace any existing states
        self.book_states[contract_id] = dict()
        for state in book_states['book_states']:
            self.handle_book_state(contract_id, state)
        self.get_top_from_book_state(contract_id)
    
    def load_books(self, contract_id):
        logging.info(f"Loading books for {contract_id}")
        if contract_id not in self.all_contracts:
            self.retrieve_contract(contract_id)
        contract = self.all_contracts[contract_id]
        if self.contract_is_expired(contract):
            logging.info(f"Skiping book loading on expired contract {contract}")
            return

        try:
            book_states = ledgerx.BookStates.get_book_states(contract_id)
            self.handle_all_book_states(book_states)
            logging.info(f"Added {len(book_states['book_states'])} open orders for {contract_id}")
        except:
            logging.exception(f"No book states for {contract_id}, perhaps it has (just) expired")
        
    def get_top_book_states(self, contract_id):
        """
        returns (top_bid_book_state, top_ask_book_state), after comparing top with all book states
        refreshing book states, if needed
        """
        top_bid_book_state = None
        top_ask_book_state = None
        top_clock = -1
        if contract_id in self.book_states:
            for mid,book_state in self.book_states[contract_id].items():
                if top_clock < book_state['clock']:
                    top_clock = book_state['clock']
        if contract_id in self.book_top and top_clock < self.book_top[contract_id]['clock'] - 2: # avoid excessive book reloading -- allow book_top to be 2 clocks ahead
            top_clock = None
        if top_clock is None or contract_id not in self.book_top or contract_id not in self.book_states:
            logging.info(f"reloading stale books for {contract_id}")
            self.load_books(contract_id)
        for mid,book_state in self.book_states[contract_id].items():
            if book_state['is_ask']:
                if top_ask_book_state is None or top_ask_book_state['price'] > book_state['price']:
                    top_ask_book_state = book_state
            else:
                if top_bid_book_state is None or top_bid_book_state['price'] < book_state['price']:
                    top_bid_book_state = book_state
        if top_bid_book_state is None or top_ask_book_state is None:
            logging.info(f"top book states are missing {top_bid_book_state} {top_ask_book_state}")
        return (top_bid_book_state, top_ask_book_state)
        

    async def async_load_books(self, contract_id):
        logging.info(f"async loading books for {contract_id}")
        self.load_books(contract_id)

    async def async_load_all_books(self, contracts):
        logging.info(f"Loading books for {contracts}")
        for contract_id in contracts:
            await self.async_load_books(contract_id)

    def delete_book_state(self, contract_id, mid):
        if contract_id not in self.book_states:
            # do not bother loading the book states
            logging.info(f"Ignoring deleted book on untraced contract {contract_id}")
            return
        if mid in self.book_states[contract_id]:
            logging.debug(f"Removing order from books {self.book_states[contract_id][mid]}")
            del self.book_states[contract_id][mid]

    def get_next_day_swap(self, asset):
        next_day_contract = None
        if asset not in self.next_day_contracts:
            for contract_id, contract in self.all_contracts.items():
                if contract['is_next_day'] and asset == contract['underlying_asset'] and not self.contract_is_expired(contract):
                    self.next_day_contracts[asset] = contract
                    break
        if asset in self.next_day_contracts:
            next_day_contract = self.next_day_contracts[asset]
            if self.contract_is_expired(next_day_contract):
                next_day_contract = None
        if next_day_contract is None:
            # get the newest one
            logging.info("Discovering the latest Next-Day swap contract")
            contracts = ledgerx.Contracts.list_all()
            for c in contracts:
                contract_id = c['id']
                if contract_id not in self.all_contracts:
                    self.add_contract(c)
                if c['is_next_day'] and 'Next-Day' in contract['label'] and c['active'] and not self.contract_is_expired(c):
                    self.next_day_contracts[c['underlying_asset']] = c
                    if asset == c['underlying_asset']:
                        next_day_contract = c
        return next_day_contract


    def add_contract(self, contract):
        if contract['date_expires'] not in self.exp_dates:
            self.add_expiration_date(contract['date_expires'])
        assert(contract['date_expires'] in self.exp_dates)
        contract_id = contract['id']
        if contract_id in self.all_contracts:
            return
        logging.info(f"add_contract: new contract {contract}")
        contract_id = contract['id']
        self.all_contracts[contract_id] = contract

        label = contract['label']
        self.label_to_contract_id[label] = contract_id
        if self.contract_is_expired(contract):
            self.expired_contracts[contract_id] = contract
            logging.info(f"contract is expired {contract}")
            if self.skip_expired:
                return
        asset = contract['underlying_asset']
        test_label = self.to_contract_label(asset, contract['date_expires'], contract['derivative_type'], contract['is_call'], contract['strike_price'])
        if label != test_label:
            logging.warn(f"different labels '{label}' vs calculated '{test_label}' for {contract}")
        if contract['is_next_day'] and 'Next-Day' in label:
            if asset not in self.next_day_contracts or not self.contract_is_expired(contract) and contract['active']:
                if asset in self.next_day_contracts:
                    current = self.next_day_contracts[asset]
                    if current['date_expires'] < contract['date_expires']:
                        self.next_day_contracts[asset] = contract
                        logging.info(f"new Next-Day swap on {asset} {contract_id} {label}")
                    else:
                        logging.info(f"ignoring old Next-Day swap on {asset} {label}")
                else:
                    self.next_day_contracts[asset] = contract
                    logging.info(f"new Next-Day swap on {asset} {contract_id} {label}")
            else:
                logging.info(f"See old Next-Day swap on {asset} {contract_id} {label}")
        if 'Put' in label:
            call_label = label.replace("Put", "Call")
            if call_label in self.label_to_contract_id:
                call_id = self.label_to_contract_id[call_label]
                self.put_call_map[contract_id] = call_id
                self.put_call_map[call_id] = contract_id
                logging.info(f"mapped Put {contract_id} {label} <=> Call {call_id} {call_label}")
            self.add_exp_strike(contract)
        elif 'Call' in label:
            put_label = label.replace("Call", "Put")
            if put_label in self.label_to_contract_id:
                put_id = self.label_to_contract_id[put_label]
                self.put_call_map[contract_id] = put_id
                self.put_call_map[put_id] = contract_id
                logging.info(f"mapped Call {contract_id} {label} <=> Put {put_id} {put_label}")
            self.add_exp_strike(contract)   

    def add_exp_strike(self, contract):
        exp = contract['date_expires']
        assert(exp in self.exp_dates)
        if exp not in self.exp_strikes:
            self.exp_strikes[exp] = dict()
        exp_asset_strikes = self.exp_strikes[exp]
        asset = contract['underlying_asset']
        if asset not in exp_asset_strikes:
            exp_asset_strikes[asset] = []
        exp_strikes = exp_asset_strikes[asset]
        strike = contract['strike_price']
        if strike not in exp_strikes:
            exp_strikes.append(strike)
            exp_strikes.sort()

    def to_contract_label(self, _asset, _exp, derivative_type, is_call = False, strike = None):
        exp = _exp.split(' ')[0] # trim off timezone
        asset = _asset
        if asset == "CBTC":
            asset = "BTC Mini"
        if derivative_type == 'future_contract':
            return f"{exp} Future {asset}"
        elif derivative_type == 'options_contract':
            if is_call:
                return f"{asset} {exp} Call ${strike//100:,}"
            else:
                return f"{asset} {exp} Put ${strike//100:,}"
        elif derivative_type == 'day_ahead_swap':
            return f"{exp} Next-Day {asset}"
        else:
            logging.warn(f"dunno derivative type {derivative_type}")

    def contract_added_action(self, action):
        assert(action['type'] == 'contract_added')
        self.add_contract(action['data'])

    def remove_contract(self, contract):
        # just flag it as expired
        assert(contract['date_expires'] in self.exp_dates)
        contract_id = contract['id']
        if contract_id in self.expired_contracts:
            return
        logging.info(f"expired contract {contract}")
        self.expired_contracts[contract_id] = contract

    def contract_removed_action(self, action):
        assert(action['type'] == 'contract_removed')
        self.remove_contract(action['data'])
            
    def trade_busted_action(self, action):
        logging.info("Busted trade {action}")
        # TODO 

    def open_positions_action(self, action):
        logging.info(f"Positions {action}")
        assert(action['type'] == 'open_positions_update')
        assert('positions' in action)
        update_basis = []
        update_all = []
        for position in action['positions']:
            contract_id = position['contract_id']
            if contract_id in self.contract_positions:
                contract_position = self.contract_positions[contract_id]
                if 'mpid' in contract_position:
                    assert(position['mpid'] == contract_position['mpid'])
                if position['size'] != contract_position['size']:
                    update_basis.append(contract_id)
                for field in ['exercised_size', 'size']:
                    contract_position[field] = position[field]
            elif position['size'] != 0 or position['exercised_size'] != 0:
                self.contract_positions[contract_id] = position
                update_all.append(contract_id)
                logging.info(f"No position for {contract_id}")
        if len(update_all) > 0:
            logging.info(f"Getting new positions for at least these new contracts {update_all}")
            needs_all = False
            for contract_id in update_all:
                if contract_id not in self.contract_positions:
                    needs_all = True
                else:
                    self.update_position(contract_id)
            if needs_all:
                self.update_all_positions()
                
        if len(update_basis) > 0:
            logging.info(f"Getting updated basis for these contracts {update_basis}")
            for contract_id in update_basis:
                self.update_position(contract_id)

    def collateral_balance_action(self, action):
        logging.info(f"Collateral {action}")
        assert(action['type'] == 'collateral_balance_update')
        assert('collateral' in action)
        assert('available_balances' in action['collateral'])
        assert('position_locked_balances' in action['collateral'])
        for balance, asset_balance in action['collateral'].items():
            for asset, val in asset_balance.items():
                if balance not in self.accounts:
                    self.accounts[balance] = dict()
                self.accounts[balance][asset] = val

    async def book_top_action(self, action) -> bool:
        assert(action['type'] == 'book_top')
        contract_id = action['contract_id']
        if contract_id == 0:
            logging.warning(f"Got erroneous book_top {action}")
            return False
        if contract_id not in self.all_contracts:
            logging.info(f"loading contract for book_top {contract_id} {action}")
            self.retrieve_contract(contract_id)
            await self.async_load_books(contract_id)
            logging.info(f"ignoring possible stale book top {action}")
            return False
        else:
            if contract_id not in self.book_top:
                logging.info(f"no books yet for booktop {contract_id} {action}")
                self.book_top[contract_id] = action
            top = self.book_top[contract_id]
            assert(contract_id == top['contract_id'])
            if top['clock'] < action['clock']:
                logging.debug(f"BookTop update {contract_id} {self.all_contracts[contract_id]['label']} {action}")
                self.book_top[contract_id] = action
                self.cost_to_close(contract_id)
                return True
            else:
                if top['clock'] == action['clock']:
                    if top['ask'] == action['ask'] and top['bid'] == action['bid']:
                        logging.debug(f"Ignored duplicate book top {action}")
                    else:
                        logging.warn(f"Found DIFFERENT book_top with same clock {top} {action}")
                else:
                    logging.warn(f"Ignored stale book top {action} kept newer {top}")
                return False

    async def heartbeat_action(self, action):
        logging.info(f"Heartbeat {action}")
        assert(action['type'] == 'heartbeat')
        if self.last_heartbeat is None:
            pass
        else:
            if self.last_heartbeat['ticks'] >= action['ticks']:
                logging.warning(f"Out of order heartbeats last={self.last_heartbeat} now={action}")
            if self.last_heartbeat['run_id'] != action['run_id']:
                logging.info("Reloading market state")
                self.clear()
                self.load_market()
        self.last_heartbeat = action

        beat_time = dt.datetime.fromtimestamp(action['timestamp'] // 1000000000, tz=self.timezone)
        now = dt.datetime.now(tz=self.timezone)
        delay = (now - beat_time).total_seconds()
        if delay > 2:
            logging.warn(f"Processed old heartbeat {delay} seconds old {action}")
            # do not perform any more work
            return
        await self.load_remaining_books(2)

    async def action_report_action(self, action):
        logging.debug(f"ActionReport {action}")
        assert(action['type'] == 'action_report')
        self.handle_order(action)

    async def handle_action(self, action):
        logging.debug(f"handle_action {action['type']}")
        if len(self.exp_dates) == 0:
            self.load_market()
        type = action['type']
        if type == 'book_top':
            await self.book_top_action(action)
        elif type == 'action_report':
            await self.action_report_action(action)
        elif type == 'heartbeat':
            await self.heartbeat_action(action)
        elif type == 'collateral_balance_update':
            self.collateral_balance_action(action)
        elif type == 'open_positions_update':
            self.open_positions_action(action)
        elif type == 'exposure_reports':
            logging.info(f"Exposure report {action}")
        elif type == 'contract_added':
            self.contract_added_action(action)
        elif type == 'contract_removed':
            self.contract_removed_action(action)
        elif type == 'trade_busted':
            self.trade_busted_action(action)
        elif 'contact_' in type:
            logging.info(f"contact change {action}")
        elif '_success' in type:
            logging.info(f"Successful {type}")
        else:
            logging.warn(f"Unknown action type {type}: {action}")

    def retrieve_contract(self, contract_id):
        contract = ledgerx.Contracts.retrieve(contract_id)["data"]
        assert(contract["id"] == contract_id)
        if contract_id not in self.all_contracts:
            logging.info(f"retrieve_contract: new contract {contract}")
            self.add_contract(contract)
        return contract  

    def set_traded_contracts(self):
        # get the list of my traded contracts
        # this may include inactive / expired contracts
        skipped = 0
        traded_contracts = ledgerx.Contracts.list_all_traded()
        logging.info(f"Got {len(traded_contracts)} traded_contracts")
        for traded in traded_contracts:
            logging.debug(f"traded {traded}")
            contract_id = traded['id']
            if contract_id not in self.all_contracts:            
                # look it up
                contract = self.retrieve_contract(contract_id)
                
            self.traded_contract_ids[contract_id] = self.all_contracts[contract_id]
            contract_label = self.all_contracts[contract_id]["label"]
            logging.debug(f"Traded {contract_id} {contract_label}")
        
    def add_transaction(self, transaction):
        logging.debug(f"transaction {transaction}")
        if transaction['state'] != 'executed':
            logging.warn(f"unknown state for transaction: {transaction}")
            return
        asset = transaction['asset']
        if asset not in self.accounts:
            self.accounts[asset] = {"available_balance": 0, "position_locked_amount": 0, "withdrawal_locked_amount" : 0}
        acct = self.accounts[asset]
        if transaction['debit_post_balance'] is not None:
            deb_field = transaction['debit_account_field_name']
            if deb_field not in acct:
                logging.warn(f"unknown balance type {deb_field}")
                acct[deb_field] = 0
            acct[deb_field] -= transaction['amount']
            assert(-transaction['amount'] == transaction['debit_post_balance'] - transaction['debit_pre_balance'])
        if transaction['credit_post_balance'] is not None:
            cred_field = transaction['credit_account_field_name']
            if cred_field not in acct:
                logging.warn(f"unknown balance type {deb_field}")
                acct[cred_field] = 0
            acct[cred_field] += transaction['amount']
            assert(transaction['amount'] == transaction['credit_post_balance'] - transaction['credit_pre_balance'])

    
    async def async_update_basis(self, contract_id, position):
        self.update_basis(contract_id, position)

    def update_basis(self, contract_id, position):
        if 'id' not in position or 'contract' not in position:
            logging.warn(f"Cannot update basis with an improper position {position}")
            self.to_update_basis[contract_id] = position
            return
        contract = position['contract']
        if contract_id != contract['id']:
            logging.warn(f"Improper match of {contract_id} to {position}")
            return

        if self.skip_expired and self.contract_is_expired(contract):
            logging.info(f"skipping basis update for expired contract {contract['label']}")
            return

        pos_id = position["id"]
        logging.info(f"updating position with trades and basis for {contract_id} {position}")
        trades = ledgerx.Positions.list_all_trades(pos_id)
        contract_label = contract['label']
        logging.info(f"got {len(trades)} trades for {contract_id} {contract_label}")
        pos = 0
        basis = 0
        for trade in trades:
            logging.debug(f"contract {contract_id} trade {trade}")
            assert(contract_id == int(trade["contract_id"]))
            if trade["side"] == "bid":
                # bought so positive basis and position delta
                basis += trade["fee"] - trade["rebate"] + trade["premium"]
                pos += trade["filled_size"]
            else:
                assert(trade["side"] == "ask")
                # sold, so negative basis and negative position delta
                basis += trade["fee"] - trade["rebate"] - trade["premium"]
                pos -= trade["filled_size"]
        #logging.debug(f"final pos {pos} basis {basis} position {position}")
        if position["type"] == "short":
            assert(pos <= 0)
        else:
            assert(position["type"] == "long")
            assert(pos >= 0)
        if pos != position['size']:
            logging.warn(f"update to position did not yield pos={pos} {position}, updating them all")
            self.update_all_positions()
            return
        position["basis"] = basis
        cost = basis / 100.0
        self.contract_positions[contract_id] = position
        if contract_id in self.to_update_basis:
            del self.to_update_basis[contract_id]

        logging.info(f"Position after {len(trades)} trade(s) {position['size']} CBTC ${cost} -- {contract_id} {contract_label}")
        

    def update_all_positions(self):
        logging.info(f"Updating all positions")
        all_positions = ledgerx.Positions.list_all()
        for pos in all_positions:
            assert('id' in pos and 'contract' in pos)
            contract = pos['contract']
            contract_id = contract['id']
            old_pos = None
            if contract_id in self.contract_positions:
                old_pos = self.contract_positions[contract_id]
                if 'basis' in old_pos and old_pos['size'] == pos['size'] and old_pos['assigned_size'] == pos['assigned_size']:
                    pos['basis'] = old_pos['basis']
            self.contract_positions[contract_id] = pos
            if 'basis' not in pos:
                logging.info(f"position for {contract_id} {contract['label']} is missing basis or changed {pos}")
                self.to_update_basis[contract_id] = pos

    async def async_update_position(self, contract_id, position = None):
        self.update_position(contract_id, position)

    def update_position(self, contract_id, position = None):
        logging.info(f"updating position for {contract_id}")
        if contract_id not in self.all_contracts:
            self.retrieve_contract(contract_id)
        if position is None and contract_id in self.contract_positions:
            position = self.contract_positions[contract_id]
        if position is None or 'id' not in position:
            logging.info(f"listing all positions as it is missing for {contract_id}")
            self.update_all_positions()
            if contract_id not in self.contract_positions:
                logging.warn(f"After updating all, still could not find a position for {contract_id}")
                return
            position = self.contract_positions[contract_id]
        if position is None or 'id' not in position:
            logging.warn(f"Could not find a postiion for {contract_id}")
            return
        
        self.update_basis(contract_id, position)
        
    def load_market(self):
        
        self.clear()
        
        # first load all active contracts, dates and meta data
        logging.info("Loading contracts")
        contracts = ledgerx.Contracts.list_all()
        self.exp_dates = unique_values_from_key(contracts, "date_expires")
        self.exp_dates.sort()
        logging.info(f"Got {len(self.exp_dates)} Expiration dates ")
        for d in self.exp_dates:
            logging.info(f"{d}")
        
        for contract in contracts:
            self.add_contract(contract)
        logging.info(f"Found {len(self.all_contracts.keys())} Contracts")

        # load my open orders
        self.orders.clear()
        for order in ledgerx.Orders.open()['data']:
            if self.mpid is None:
                self.mpid = order['mpid']
            if self.cid is None:
                self.cid = order['cid']
            assert(self.mpid == order['mpid'])
            assert(self.cid == order['cid'])
            self.handle_order(order)
        logging.info(f"Found {len(self.orders.keys())} Open Orders")

        # load the set of contracts traded in my account
        self.set_traded_contracts()

    async def load_all_transactions(self):
        # load transactions for and get account balances
        logging.info("Loading transactions for account balances")
        transactions = ledgerx.Transactions.list_all()
        for transaction in transactions:
            self.add_transaction(transaction)
        logging.info(f"Loaded {len(transactions)} transactions")
        logging.info(f"Accounts: {self.accounts}")
        
           
    async def load_positions_orders_and_books(self):

        # TODO is this still needed? --- await self.load_all_transactions()

        # get the positions for the my traded contracts
        skipped = 0
        self.update_all_positions()

        # get all the trades for each of my positions
        # and calculate basis & validate the balances
        for contract_id, position in self.contract_positions.items():
            await self.async_update_position(contract_id, position)
                        
        if not self.skip_expired:
            # zero out expired positions -- they no longer exist
            for contract_id, expired in self.expired_contracts.items():
                if contract_id in self.contract_positions:
                    position = self.contract_positions[contract_id]
                    position['expired_size'] = position['size']
                    position['size'] = 0
                    logging.info(f"Adjusted expired position {position}")

        
        open_contracts = list(self.contract_positions.keys())
        open_contracts.sort()
        logging.info(f"Have the following {len(open_contracts)} Open Positions")
        for contract_id in open_contracts:
            contract = self.all_contracts[contract_id]
            label = contract['label']
            position = self.contract_positions[contract_id]
            if position['size'] == 0:
                continue
            cost = None
            if 'basis' in position:
                cost = position['basis'] / 100.0
            logging.info(f"{label} {position['size']} {cost}")

        await self.async_load_all_books(self.traded_contract_ids.keys())

        # Calculate net to close all positions 
        total_net_close = 0
        total_net_basis = 0
        for contract_id, position in self.contract_positions.items():
            label = self.all_contracts[contract_id]['label']
            basis = None
            size = position['size']
            if 'basis' in position:
                basis = position['basis']
                total_net_basis += basis
            if contract_id in self.book_top:
                top = self.book_top[contract_id]
                if size > 0:
                    # sell at bid
                    bid = self.bid(top)
                    if bid is not None:
                        fee = self.fee(bid,size)
                        sale = (size * bid - fee) // 10000
                        total_net_close += sale
                        logging.info(f"Sell for ${sale}, {size} of {label} at top bid ${bid//100} with basis ${basis//100}, net ${(sale - basis//100)//1}")
                    else:
                        logging.info(f"No bid buyers for {size} of {label}")
                elif size < 0:
                    # buy at ask
                    ask = self.ask(top)
                    if ask is not None:
                        fee = self.fee(ask,size)
                        purchase = (size * ask + fee) // 10000
                        total_net_close += purchase
                        logging.info(f"Buy for ${-purchase}, {-size} of {label} at top ask ${ask//100} with basis ${basis//100}, net ${(purchase - basis/100)//1}")
                    else:
                        logging.info(f"No ask sellers for {size} of {label}")
        logging.info(f"Net to close ${total_net_close} with basis ${total_net_basis//100} = ${total_net_close - total_net_basis//100} to close all positions at best (top) price.  Did not explore all books for size")


    async def load_remaining_books(self, max = 2):
        count = 0
        updated = []
        for contract_id,pos in self.to_update_basis.items():
            logging.info(f"requested update basis on {contract_id} {pos}")
            if 'id' in pos and 'contract' in pos:
                await self.async_update_basis(contract_id, pos)
            else:
                self.update_position(contract_id)
            updated.append(contract_id)
            count = count + 1
            if max > 0 and count >= max:
                    break
        for contract_id in updated:
            if contract_id in self.to_update_basis:
                del self.to_update_basis[contract_id]
        if count > 0:
            logging.info(f"Updated {count} position basis")
        if max > 0 and count >= max:
            return
        for contract_id, contract in self.all_contracts.items():
            if self.contract_is_expired(contract):
                continue
            if contract_id not in self.book_states:
                logging.info(f"Loading books for {contract_id}")
                await self.async_load_books(contract_id)
                count = count + 1
                if max > 0 and count >= max:
                    break
        if count > 0:
            logging.info(f"Done loading {count} positions and books")

    def _run_websocket_server(self, callback, include_api_key, repeat_server_port):
        return ledgerx.WebSocket.run_server(callback, include_api_key=include_api_key, repeat_server_port=repeat_server_port)

    async def __start_websocket_and_run(self, executor, include_api_key=False, repeat_server_port=None):
        loop = asyncio.get_running_loop()
        
        task1 = await loop.run_in_executor(executor, self._run_websocket_server, self.handle_action, include_api_key, repeat_server_port)
        task2 = await loop.run_in_executor(executor, self.load_positions_orders_and_books )
        await asyncio.gather( task2, task1 ) 

    def start_websocket_and_run(self, include_api_key=False, repeat_server_port=None):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        logging.info(f"Starting market_state = {self}")
        self.load_market()
        loop = asyncio.get_event_loop()
        threading.Thread(target=loop.run_until_complete, args=(self.__start_websocket_and_run(executor, include_api_key, repeat_server_port),)).start()
