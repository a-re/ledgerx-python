import asyncio
import logging
import json
import ledgerx

from ledgerx.util import unique_values_from_key

class MarketState:

    # state of market and positions
    all_contracts = dict()
    traded_contract_ids = dict()
    expired_contracts = dict()
    contract_positions = dict()
    accounts = dict()
    exp_dates = list()
    orders = dict()             # my orders dict{contract_id: order}
    book_states = dict()        # a dict{contract_id : dict{mid : order}}
    book_top = dict()           # a dict{contract_id : top}
    last_heartbeat = None
    mpid = None
    cid = None

    next_day_contract_id = None
    label_to_contract_id = dict() # dict(contract['label']: contract_id)
    put_call_map = dict()         # dict(contract_id: contract_id) put -> call and call -> put

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
        self.next_day_contract_id = None
        self.label_to_contract_id = dict()
        self.put_call_map = dict()



    def add_expiration_date(self, date):
        assert(date not in self.exp_dates)
        self.exp_dates.append(date)
        self.exp_dates.sort()

    def replace_existing_order(self, order):
        # replace if clock is larger
        mid = order['mid']
        exists = mid in self.orders
        if exists:
            existing = self.orders[mid]
            contract_id = order['contract_id']
            contract = self.all_contracts[contract_id]
            if existing['clock'] <= order['clock'] and existing['ticks'] < order['ticks']:
                self.orders[mid] = order
                logging.info(f"Replaced existing order on {contract['label']} {existing} to {order}")
            else:
                if existing['ticks'] == order['ticks']:
                    logging.debug("Got duplicate order  on {contract['label']} {existing} vs {order}")
                else:
                    logging.warn(f"existing order on {contract['label']} {existing} is newer {order}, ignoring")
    
    def handle_order(self, order):
        mid = order['mid']
        exists = mid in self.orders
        existing = None
        if exists:
            existing = self.orders[mid]
        if exists:
            if 'mpid' in order and self.mpid != order['mpid']:
                logging.warn("different mpid {self.mpid} for mid {mid} {order} {self.orders[mid]}")
            #assert(self.mpid == order['mpid']) # only my orders are in self.orders
            #assert(self.cid == order['cid'])
        status = order['status_type']
        contract_id = order['contract_id']
        contract = self.all_contracts[contract_id]
        label = contract['label']
        if status == 200:
            # A resting order was inserted
            if exists:
                self.replace_existing_order(order)
            else:
                if self.mpid is not None and 'mpid' in order and self.mpid == order['mpid']:
                    self.orders[mid] = order
                    logging.info(f"adding/updating my order on {label} {order}")
                else:
                    logging.debug(f"Ignoring 3rd party order tracking on {label} order {order}")
                assert(order['status_type'] == 200)
            self.handle_book_state(contract_id, order)
        elif status == 201:
            # a cross (trade) occured
            logging.info(f"Cross trade {existing} {order}")
            self.replace_existing_order(order)
            self.handle_book_state(contract_id, order)
        elif status == 202:
            # A market order was not filled
            logging.warn(f"dunno how to handle not filled market order on {label} {existing} {order}")
        elif status == 203:
            # cancelled
            if exists:
                logging.info(f"Deleting cancelled order on {label} {existing} {order}")
                del self.orders[mid]
            else:
                logging.debug(f"Ignoring cancelled order on {label} {order}")
            # handle copy in book_states, if needed
            self.delete_book_state(contract_id, mid)
        elif status == 300:
            logging.info(f"Acknowledged on {label} {existing} {order}")
        elif status == 610:
            # expired
            logging.info(f"Expired on {label} {existing} {order}")
            if exists:
                del self.orders[mid]
            self.delete_book_state(contract_id, mid)
        elif status >= 600:
            logging.warn(f"invalid or rejected order {order}")
            if exists:
                del self.orders[mid]

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
            if book_state['clock'] < books[mid]['clock']:
                logging.info(f"Ignoring old book_state={book_state} orig={books[mid]}")
                return
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
        logging.debug(f"new contract {contract}")
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
        #TODO

    def collateral_balance_action(self, action):
        logging.info(f"Collateral {action}")
        #TODO

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
                logging.info(f"BookTop update {contract_id} {self.all_contracts[contract_id]['label']} {action}")
                self.book_top[contract_id] = action
            else:
                if top['clock'] == action['clock']:
                    if top['ask'] == action['ask'] and top['bid'] == action['bid']:
                        logging.debug(f"Ignored duplicate book top {action}")
                    else:
                        logging.warn(f"Found DIFFERNT book_top with same clock {top} {action}")
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
                self.market_state.clear()
                self.market_state.load_market()
        self.last_heartbeat = action

    def action_report_action(self, action):
        logging.debug(f"ActionReport {action}")
        assert(action['type'] == 'action_report')
        contract_id = action['contract_id']
        self.handle_order(action)

    def _handle_action(self, action):
        logging.debug(f"handle_action {action['type']}")
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
            logging.info("Added new contract {contract}")
            self.all_contracts[contract_id] = contract
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
            logging.debug(f"Position {contract_id} {contract_label} {pos}")
        logging.info(f"Skipped {skipped} expired positions")

        # get all the trades for each of my positions
        # and calculate basis & validate the balances
        for contract_id, position in self.contract_positions.items():
            pos_id = position["id"]
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
            assert(pos == position['size'])
            position["basis"] = basis
            cost = basis / 100.0

            logging.info(f"Position after {len(trades)} trade(s) {position['size']} CBTC ${cost} -- {contract_id} {contract_label}")
            
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

        # start websocket listener


        


