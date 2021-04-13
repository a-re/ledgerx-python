import asyncio
import logging
import json
import ledgerx

import numpy_financial as npf
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
    orders = dict()             # all orders in the market dict{contract_id: dict{mid: order}}
    book_states = dict()        # all books in the market  dict{contract_id : dict{mid : order}}
    book_top = dict()           # all top books in the market dict{contract_id : top}
    to_update = list()
    label_to_contract_id = dict() # dict(contract['label']: contract_id)
    put_call_map = dict()         # dict(contract_id: contract_id) put -> call and call -> put
    last_heartbeat = None
    mpid = None
    cid = None
    next_day_contract_id = None

    # Constants
    risk_free = 0.005 # 0.5% risk free interest
    timezone = dt.timezone.utc
    strptime_format = "%Y-%m-%d %H:%M:%S%z"
    seconds_per_year = 3600.0 * 24.0 * 365.0  # ignore leap year, okay?

    def clear(self):
        self.all_contracts = dict()
        self.traded_contract_ids = dict()
        self.expired_contracts = dict()
        self.contract_positions = dict()
        self.accounts = dict()
        self.exp_dates = list()
        self.orders = dict()
        self.book_states = dict()
        self.book_top = dict()
        self.to_update = list()
        self.label_to_contract_id = dict()
        self.put_call_map = dict()
        self.next_day_contract_id = None

        # keep any previous heartbeats, mpid and cid

    def cost_to_close(self, contract_id):
        "returns tuple (low, high)"
        if contract_id not in self.contract_positions:
            return (0, 0)
        position = self.contract_positions[contract_id]
        size = position['size']
        if size == 0:
            return (0,0)
        if contract_id not in self.book_top:
            self.load_books(contract_id)
        top = self.book_top[contract_id]
        bid = top['bid']
        ask = top['ask']
        fee = 15 * abs(size)
        mid = (ask + bid) / 2
        cost = (fee + mid * size) / 10000
        basis = None
        net = None
        if 'basis' in position:
            basis = position['basis'] / 100.0
            if size < 0:
                net = int((fee + ask * size) / 10000.0 - basis)
            else:
                net = int((fee + bid * size) / 10000.0 - basis)
        if basis is not None:
            logging.info(f"net ${net}: cost ${cost} - basis ${basis} to close {size} of {self.all_contracts[contract_id]['label']} at ${bid//100} to ${ask//100}")
        else:
            logging.warn(f"No basis for ${cost} to close {size} of {self.all_contracts[contract_id]['label']} at ${bid//100} to ${ask//100}")
        if size < 0:
            return ((fee + bid * size) / 10000.0, (fee + ask * size) / 10000.0)
        else:
            return ((fee + ask * size) / 10000.0, (fee + bid * size) / 10000.0)

    def ask(self, top_book):
        if 'ask' in top_book:
            ask = top_book['ask']
            if ask is not None and ask != 0:
                return ask
        return None

    def bid(self, top_book):
        if 'bid' in top_book:
            bid = top_book['bid']
            if bid is not None and bid != 0:
                return bid
        return None

    def is_same_option_date(self, contract_a, contract_b):
        return 'is_call' in contract_a and 'is_call' in contract_b and contract_a['is_call'] == contract_b['is_call'] and \
            contract_a['date_expires'] == contract_b['date_expires'] and \
                 contract_a['derivative_type'] == contract_b['derivative_type'] and \
                     contract_a['underlying_asset'] == contract_b['underlying_asset']

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
        
        if self.next_day_contract_id is not None and self.next_day_contract_id in self.book_top:
            top = self.book_top[self.next_day_contract_id]
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

    def put_call_parity(self, contract_id):
        # look for put call parity arbitrage: C + PV(x) = P + S
        # Buy Call @ask + PV(strike cash) < Sell Put @bid + Sell Asset @bid
        # Sell Call @bid + PV(strike cash) > Buy Put @ask + Buy Asset @ask
        contract = self.all_contracts[contract_id]
        if contract['derivative_type'] != 'options_contract' or self.next_day_contract_id is None:
            return
        other_contract_id = self.put_call_map[contract_id]
        call_id = contract_id
        put_id = other_contract_id
        call = contract
        put = None
        if call['is_call']:
            call = contract
            put = self.all_contracts[put_id]
        else:
            call_id = other_contract_id
            call = self.all_contracts[call_id]
            put_id = contract_id
            put = contract
        #logging.info(f"{call} {put}")
        assert(call['id'] == call_id)
        assert(put['id'] == put_id)
        assert(call['is_call'] and not put['is_call'])
        if call_id not in self.book_top:
            self.load_books([call_id])
        if put_id not in self.book_top:
            self.load_books([put_id])
        if self.next_day_contract_id not in self.book_top:
            self.load_books([self.next_day_contract_id])
        if call_id in self.book_top and put_id in self.book_top and self.next_day_contract_id in self.book_top:
            pass
        else:
            logging.warn(f"Could not find both call and put contracts in book_top {call} {put}")
            return
        # put call parity
        top_call = self.book_top[call_id]
        top_put = self.book_top[put_id]
        top_swap = self.book_top[self.next_day_contract_id]

        strike = call['strike_price']
        now = dt.datetime.now(self.timezone)
        exp = dt.datetime.strptime(call['date_expires'], self.strptime_format)
        assert(exp > now)
        t = (exp - now).total_seconds() / self.seconds_per_year
        pv = int(npf.pv(rate=self.risk_free, fv=strike, pmt=0, nper=t))
        assert(pv < 0)
        
        logging.debug(f"Testing call {top_call} put {top_put} swap {top_swap} {call} {put}")
        call_ask = self.ask(top_call)
        call_bid = self.bid(top_call)
        put_ask = self.ask(top_put)
        put_bid = self.bid(top_put)
        swap_ask = self.ask(top_swap)
        swap_bid = self.bid(top_swap)
        exclaim = ""
        if self.is_qualified_covered_call(call_id):
            exclaim = "Qualified "
        if call_bid is not None and put_ask is not None and swap_ask is not None:
            sell_call = call_bid - pv
            buy_put = -swap_ask - put_ask
            assert(buy_put < 0 and sell_call > 0)
            profit = sell_call + buy_put
            if profit > 0:
                capital = (0 - buy_put - call_bid)
                return_pct = profit / capital
                return_pct_per_year = return_pct / t
                rpf = "{:#.3g}%".format(return_pct*100.0)
                rpfy = "{:#.3g}%".format(return_pct_per_year*100.0)
                if profit >= 100 and return_pct_per_year > 0.10:
                    exclaim = exclaim + "Profitable "
                if return_pct_per_year > 0.125:
                    logging.info(f"P/C Arbitrage {exclaim}${profit//100} on ${capital//100} {rpf}/{rpfy}: Sell Call @${call_bid//100} / Buy Put @${put_ask//100}; Buy BTC @${swap_ask//100} -- Strike ${strike//100} {exp}. sell ${sell_call//100} buy ${buy_put//100} pv ${pv//100} days {(exp-now).days} {call['label']} -- top_call {top_call} top_put {top_put} top_swap {top_swap}")
            else:
                logging.debug(f"No arbitrage ${profit//100} Sell Call / Buy Put ${strike//100} {exp}. sell ${sell_call//100} buy ${buy_put//100} pv ${pv//100} days {(exp-now).days} {call['label']} -- top_call {top_call} top_put {top_put} top_swap {top_swap}")
        if call_ask is not None and put_bid is not None and swap_bid is not None:
            buy_call = pv - call_ask
            sell_put = put_bid + swap_bid
            assert(buy_call < 0 and sell_put > 0)
            profit = sell_put + buy_call
            if profit > 0:
                capital = (0 - buy_call + strike - put_bid)
                return_pct = profit / capital
                return_pct_per_year = return_pct / t
                rpf = "{:#.3g}%".format(return_pct*100.0)
                rpfy = "{:#.3g}%".format(return_pct_per_year*100.0)
                if profit > 100 and return_pct_per_year > 0.10:
                    exclaim = exclaim + "Profitable "
                if return_pct_per_year > 0.125:
                    logging.info(f"P/C Arbitrage {exclaim}${profit//100} on ${capital//100} {rpf}/{rpfy}: Sell Put @${put_bid//100}; Sell BTC @${swap_bid/100} / Buy Call @${call_ask/100} -- Strike ${strike//100} {exp}. sell ${sell_put//100} buy ${buy_call//100} pv ${pv//100} days {(exp-now).days} {call['label']} -- top_call {top_call} top_put {top_put} top_swap {top_swap}")
            else:
                logging.debug(f"No arbitrage ${profit//100} Sell Put / Buy Call ${strike//100} {exp}. sell ${sell_put//100} buy ${buy_call//100} pv ${pv//100} days {(exp-now).days} {call['label']} -- top_call {top_call} top_put {top_put} top_swap {top_swap}")

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
            logging.info(f"traded order had not been tracked yet! {order}")
            self.insert_new_order(order)
            exists = True
            existing = contract_orders[mid]
        
        logging.debug(f"handle_order on {label} {order}")
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
                #if contract_id not in self.contract_positions:
                #    self.contract_positions[contract_id] = dict()
                #position = self.contract_positions[contract_id]
                
                if order['is_ask']:
                    logging.info(f"Observed sale of {delta_pos} for ${delta_basis//100} on {contract_id} {label} {order}")
                    # sold
                    #position['size'] -= delta_pos
                    #position['basis'] -= delta_basis
                else:
                    logging.info("Observed purchase of {delta_pos} for ${delta_basis//100} on {contract_id} {label} {order}/mi")
                    # bought
                    #position['size'] += delta_pos
                    #position['basis'] += delta_basis
                #position['basis'] += 15 * delta_pos # approximate fee

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
            self.load_books([contract_id])
        books = self.book_states[contract_id]
        ask = None
        bid = None
        if contract_id not in self.all_contracts:
            self.retrieve_contract(contract_id)
        contract = self.all_contracts[contract_id]
        logging.debug(f"get_top_from_book_state contract_id {contract_id} contract {contract} books {books}")
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
        book_top = dict(ask= ask, bid= bid, contract_id= contract_id, contract_type= None, clock= -1, type= 'book_top')
        self.book_top[contract_id] = book_top
        logging.info(f"Top for {contract_id} {contract['label']} {book_top}")
        return book_top
        
    def handle_book_state(self, contract_id, book_state):
        logging.debug(f"handle_book_state {contract_id} {book_state}")
        assert('mid' in book_state)
        if contract_id not in self.book_states:
            self.load_books([contract_id])
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
    
    def load_books(self, contracts):
        for contract_id in contracts:
            book_states = ledgerx.BookStates.get_book_states(contract_id)
            self.handle_all_book_states(book_states)
            logging.info(f"Added {len(book_states['book_states'])} open orders for {contract_id}")

    def delete_book_state(self, contract_id, mid):
        if contract_id not in self.book_states:
            # do not bother loading the book states
            logging.info(f"Ignoring deleted book on untraced contract {contract_id}")
            return
        if mid in self.book_states[contract_id]:
            logging.debug(f"Removing order from books {self.book_states[contract_id][mid]}")
            del self.book_states[contract_id][mid]

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
        if 'Next-Day' in label:
            self.next_day_contract_id = contract_id
            logging.info(f"Next-Day swap {contract_id} {label}")
        if 'Put' in label:
            call_label = label.replace("Put", "Call")
            if call_label in self.label_to_contract_id:
                call_id = self.label_to_contract_id[call_label]
                self.put_call_map[contract_id] = call_id
                self.put_call_map[call_id] = contract_id
                logging.info(f"mapped Put {contract_id} {label} <=> Call {call_id} {call_label}")
        elif 'Call' in label:
            put_label = label.replace("Call", "Put")
            if put_label in self.label_to_contract_id:
                put_id = self.label_to_contract_id[put_label]
                self.put_call_map[contract_id] = put_id
                self.put_call_map[put_id] = contract_id
                logging.info(f"mapped Call {contract_id} {label} <=> Put {put_id} {put_label}")       

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
            all_positions = ledgerx.Positions.list_all()
            for pos in all_positions:
                contract_id = pos["contract"]['id']
                if contract_id not in self.contract_positions or contract_id in update_all:
                    self.update_position(contract_id, pos)
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

    def book_top_action(self, action):
        assert(action['type'] == 'book_top')
        contract_id = action['contract_id']
        if contract_id == 0:
            logging.warning(f"Got erroneoue book_top {action}")
            return
        if contract_id not in self.all_contracts:
            logging.info(f"loading contract for book_top {contract_id} {action}")
            self.retrieve_contract(contract_id)
            self.load_books([contract_id])
            logging.info(f"ignoring possible stale book top {action}")
        else:
            if contract_id not in self.book_top:
                self.load_books([contract_id])
            top = self.book_top[contract_id]
            assert(contract_id == top['contract_id'])
            if top['clock'] < action['clock']:
                logging.debug(f"BookTop update {contract_id} {self.all_contracts[contract_id]['label']} {action}")
                self.book_top[contract_id] = action
                self.cost_to_close(contract_id)
                self.put_call_parity(contract_id)
            else:
                if top['clock'] == action['clock']:
                    if top['ask'] == action['ask'] and top['bid'] == action['bid']:
                        logging.debug(f"Ignored duplicate book top {action}")
                    else:
                        logging.warn(f"Found DIFFERENT book_top with same clock {top} {action}")
                else:
                    logging.warn(f"Ignored stale book top {action} kept newer {top}")

    def heartbeat_action(self, action):
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
        if len(self.to_update):
            copy = self.to_update
            self.to_update = []
            for contract_id in copy:
                self.update_position(contract_id)

    def action_report_action(self, action):
        logging.debug(f"ActionReport {action}")
        assert(action['type'] == 'action_report')
        self.handle_order(action)

    def _handle_action(self, action):
        logging.debug(f"handle_action {action['type']}")
        if len(self.exp_dates) == 0:
            self.load_market()
        type = action['type']
        if type == 'book_top':
            self.book_top_action(action)
        elif type == 'action_report':
            self.action_report_action(action)
        elif type == 'heartbeat':
            self.heartbeat_action(action)
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
    
    def handle_action(self, action):
        try:
            self._handle_action(action)
        except:
            logging.exception(f"Problem with action {action}.  Continuing...")

    def retrieve_contract(self, contract_id):
        contract = ledgerx.Contracts.retrieve(contract_id)["data"]
        assert(contract["id"] == contract_id)
        if contract_id not in self.all_contracts:
            logging.info(f"retrieve_contract: new contract {contract}")
            self.add_contract(contract)
        return contract  

    def set_traded_contracts(self, skip_expired):
        # get the list of my traded contracts
        # this may include inactive / expired contracts
        skipped = 0
        traded_contracts = ledgerx.Contracts.list_all_traded()
        logging.info(f"Got {len(traded_contracts)} traded_contracts")
        for traded in traded_contracts:
            logging.debug(f"traded {traded}")
            contract_id = traded['id']
            if contract_id not in self.all_contracts:
                if skip_expired:
                    skipped += 1
                    continue                
                # look it up
                contract = self.retrieve_contract(contract_id)
                logging.info(f"Added expired contract {contract_id} {contract}")
                self.expired_contracts[contract_id] = contract
                
            self.traded_contract_ids[contract_id] = self.all_contracts[contract_id]
            contract_label = self.all_contracts[contract_id]["label"]
            logging.debug(f"Traded {contract_id} {contract_label}")
        logging.info(f"skipped {skipped} expired but traded contracts")
        
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
                logging.warn(f"unknownd balance type {deb_field}")
                acct[deb_field] = 0
            acct[deb_field] -= transaction['amount']
            assert(-transaction['amount'] == transaction['debit_post_balance'] - transaction['debit_pre_balance'])
        if transaction['credit_post_balance'] is not None:
            cred_field = transaction['credit_account_field_name']
            if cred_field not in acct:
                logging.warn(f"unknownd balance type {deb_field}")
                acct[cred_field] = 0
            acct[cred_field] += transaction['amount']
            assert(transaction['amount'] == transaction['credit_post_balance'] - transaction['credit_pre_balance'])

    def update_position(self, contract_id, position = None):
        if position is None:
            all_positions = ledgerx.Positions.list_all()
            for pos in all_positions:
                if contract_id == pos["contract"]['id']:
                    position = pos
                    break
        if position is None:
            logging.warn(f"Could not find a postiion for {contract_id}")
            return
        
        pos_id = position["id"]
        logging.info(f"updating position for {contract_id} {position}")
        trades = ledgerx.Positions.list_all_trades(pos_id)
        contract_label = self.all_contracts[contract_id]['label']
        logging.debug(f"got {len(trades)} trades for {contract_id} {contract_label}")
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
            logging.warn(f"update to position did not yield pos={pos} {position}, scheduling for after next heartbeat")
            self.to_update.append(contract_id)
        position["basis"] = basis
        cost = basis / 100.0
        self.contract_positions[contract_id] = position

        logging.info(f"Position after {len(trades)} trade(s) {position['size']} CBTC ${cost} -- {contract_id} {contract_label}")

    def load_market(self, skip_expired : bool = True):
        
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
        self.set_traded_contracts(skip_expired)
        
        # load transactions for and get account balances
        logging.info("Loading transactions for account balances")
        transactions = ledgerx.Transactions.list_all()
        for transaction in transactions:
            self.add_transaction(transaction)
        logging.info(f"Loaded {len(transactions)} transactions")
        logging.info(f"Accounts: {self.accounts}")
        
        # get the positions for the my traded contracts
        skipped = 0
        all_positions = ledgerx.Positions.list_all()
        for pos in all_positions:
            contract_id = pos["contract"]['id']
            logging.debug(f"pos {pos}")
            if skip_expired and contract_id not in self.all_contracts:
                skipped += 1
                continue
            self.contract_positions[contract_id] = pos

            contract_label = self.all_contracts[contract_id]["label"]
            logging.info(f"Position {contract_id} {contract_label} {pos}")
        logging.info(f"Skipped {skipped} expired positions")

        # get all the trades for each of my positions
        # and calculate basis & validate the balances
        for contract_id, position in self.contract_positions.items():
            self.update_position(contract_id, position)
                        
        if not skip_expired:
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
            cost = position['basis'] / 100.0
            logging.info(f"{label} {position['size']} {cost}")

        self.load_books(self.traded_contract_ids.keys())

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
                fee = abs(size) * 15
                if size > 0:
                    # sell at bid
                    bid = self.bid(top)
                    if bid is not None:
                        sale = (size * bid - fee) // 10000
                        total_net_close += sale
                        logging.info(f"Sell for ${sale}, {size} of {label} at top bid ${bid//100} with basis ${basis//100}, net ${(sale - basis//100)//1}")
                    else:
                        logging.info(f"No bid buyers for {size} of {label}")
                elif size < 0:
                    # buy at ask
                    ask = self.ask(top)
                    if ask is not None:
                        purchase = (size * ask + fee) // 10000
                        total_net_close += purchase
                        logging.info(f"Buy for ${-purchase}, {-size} of {label} at top ask ${ask//100} with basis ${basis//100}, net ${(purchase - basis/100)//1}")
                    else:
                        logging.info(f"No ask sellers for {size} of {label}")
        logging.info(f"Net to close ${total_net_close} with basis ${total_net_basis//100} = ${total_net_close - total_net_basis//100} to close all positions at best (top) price.  Did not explore all books for size")



        


