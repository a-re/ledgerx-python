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

    @classmethod
    def add_expiration_date(cls, date):
        assert(date not in cls.exp_dates)
        cls.exp_dates.append(date)
        cls.exp_dates.sort()

    @classmethod
    def add_contract(cls, contract):
        if contract['date_expires'] not in cls.exp_dates:
            cls.add_expiration_date(contract['date_expires'])
        assert(contract['date_expires'] in cls.exp_dates)
        contract_id = contract['id']
        if contract_id in cls.all_contracts:
            return
        logging.debug(f"new contract {contract}")
        contract_id = contract['id']
        cls.all_contracts[contract_id] = contract

    @classmethod
    def contract_added_action(cls, action):
        assert(action['type'] == 'contract_added')
        cls.add_contract(action['data'])

    @classmethod
    def remove_contract(cls, contract):
        # just flag it as expired
        assert(contract['date_expires'] in cls.exp_dates)
        contract_id = contract['id']
        if contract_id in cls.expired_contracts:
            return
        logging.info(f"expired contract {contract}")
        cls.expired_contracts[contract_id] = contract

    @classmethod
    def contract_removed_action(cls, action):
        assert(action['type'] == 'contract_removed')
        cls.remove_contract(action['data'])
            
    @classmethod
    def trade_busted_action(cls, action):
        logging.info("Busted trade {action}")
        # TODO 

    @classmethod
    def positions_action(cls, action):
        logging.info(f"Positions {action}")
        #TODO

    @classmethod
    def collateral_action(cls, action):
        logging.info(f"Collateral {action}")
        #TODO

    @classmethod
    def book_top_action(cls, action):
        logging.info(f"BookTop {action}")

    @classmethod
    def heartbeat_action(cls, action):
        logging.info(f"Hearbeat {action}")

    @classmethod
    def handle_action(cls, action):
        type = action['type']
        if type == 'book_top':
            cls.book_top_action(action)
        elif type == 'collateral':
            cls.collateral_action(action)
        elif type == 'positions':
            cls.positions_action(action)
        elif type == 'contract_added':
            cls.contract_added_action(action)
        elif type == 'contract_removed':
            cls.contract_removed_action(action)
        elif type == 'trade_busted':
            cls.trade_busted_action(action)
        elif 'contact_' in type:
            logging.info(f"contact change {action}")
        else:
            logging.warn("Unknown action type {type}: {action}")
    

    @classmethod
    def set_traded_contracts(cls, skip_expired):
        # get the list of my traded contracts
        # this may include inactive / expired contracts
        skipped = 0
        traded_contracts = ledgerx.Contracts.list_all_traded()
        logging.info(f"Got {len(traded_contracts)} traded_contracts")
        for traded in traded_contracts:
            logging.debug(f"traded {traded}")
            contract_id = traded['id']
            if contract_id not in cls.all_contracts:
                if skip_expired:
                    skipped += 1
                    continue                
                # look it up
                contract = ledgerx.Contracts.retrieve(contract_id)["data"]
                logging.info(f"Added expired contract {contract_id} {contract}")
                assert(contract["id"] == contract_id)
                cls.all_contracts[contract_id] = contract
                cls.expired_contracts[contract_id] = contract
                
            cls.traded_contract_ids[contract_id] = cls.all_contracts[contract_id]
            contract_label = cls.all_contracts[contract_id]["label"]
            logging.debug(f"Traded {contract_id} {contract_label}")
        logging.info(f"skipped {skipped} expired but traded contracts")
        
    @classmethod
    def add_transaction(cls, transaction):
        logging.debug(f"transaction {transaction}")
        if transaction['state'] != 'executed':
            logging.warn(f"unknown state for transaction: {transaction}")
            return
        asset = transaction['asset']
        if asset not in cls.accounts:
            cls.accounts[asset] = {"available_balance": 0, "position_locked_amount": 0, "withdrawal_locked_amount" : 0}
        acct = cls.accounts[asset]
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


    @classmethod
    def loadMarket(cls, skip_expired : bool = False):
        
        # first load all active contracts, dates and meta data
        logging.info("Loading contracts")
        contracts = ledgerx.Contracts.list_all()
        cls.exp_dates = unique_values_from_key(contracts, "date_expires")
        cls.exp_dates.sort()
        logging.info(f"Got {len(cls.exp_dates)} Expiration dates ")
        for d in cls.exp_dates:
            logging.info(f"{d}")
        
        for contract in contracts:
            cls.add_contract(contract)
        logging.info(f"Found {len(cls.all_contracts.keys())} Contracts")

        cls.set_traded_contracts(skip_expired)
        
        # load transactions for and get account balances
        logging.info("Loading transactions for account balances")
        transactions = ledgerx.Transactions.list_all()
        for transaction in transactions:
            cls.add_transaction(transaction)
        logging.info(f"Loaded {len(transactions)} transactions")
        logging.info(f"Accounts: {cls.accounts}")
        

        # get the positions for the my traded contracts
        skipped = 0
        all_positions = ledgerx.Positions.list_all()
        for pos in all_positions:
            contract_id = pos["contract"]['id']
            logging.debug(f"pos {pos}")
            if skip_expired and contract_id not in cls.all_contracts:
                skipped += 1
                continue
            cls.contract_positions[contract_id] = pos

            contract_label = cls.all_contracts[contract_id]["label"]
            logging.debug(f"Position {contract_id} {contract_label} {pos}")
        logging.info(f"Skipped {skipped} expired positions")

        # get all the trades for each of my positions
        # and calculate basis & validate the balances
        for contract_id, position in cls.contract_positions.items():
            pos_id = position["id"]
            trades = ledgerx.Positions.list_all_trades(pos_id)
            contract_label = cls.all_contracts[contract_id]['label']
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
            for contract_id, expired in cls.expired_contracts.items():
                if contract_id in cls.contract_positions:
                    position = cls.contract_positions[contract_id]
                    position['expired_size'] = position['size']
                    position['size'] = 0
                    logging.info(f"Adjusted expired position {position}")

        
        open_contracts = list(cls.contract_positions.keys())
        open_contracts.sort()
        logging.info(f"Have the following {len(open_contracts)} Open Positions")
        for contract_id in open_contracts:
            contract = cls.all_contracts[contract_id]
            label = contract['label']
            position = cls.contract_positions[contract_id]
            if position['size'] == 0:
                continue
            cost = position['basis'] / 100.0
            logging.info(f"{label} {position['size']} {cost}")


        


