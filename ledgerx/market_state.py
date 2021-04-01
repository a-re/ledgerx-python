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
    exp_dates = dict()

    @classmethod
    def loadMarket(cls, skip_expired : bool = False):
        
        # first load all active contracts, dates and meta data
        logging.info("Loading contracts")
        contracts = ledgerx.Contracts.list_all()
        cls.exp_dates = unique_values_from_key(contracts, "date_expires")
        logging.info(f"Expiration dates {cls.exp_dates}")
        for contract in contracts:
            logging.info(f"new contract {contract}")
            assert(contract['date_expires'] in cls.exp_dates)
            contract_id = contract['id']
            cls.all_contracts[contract_id] = contract
            
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

        # get the positions for the my traded contracts
        skipped = 0
        all_positions = ledgerx.Positions.list_all()
        for pos in all_positions:
            contract_id = pos["contract"]['id']
            logging.info(f"pos {pos}")
            if skip_expired and contract_id not in cls.all_contracts:
                skipped += 1
                continue
            cls.contract_positions[contract_id] = pos

            contract_label = cls.all_contracts[contract_id]["label"]
            logging.info(f"Position {contract_id} {contract_label} {pos}")
        logging.info(f"Skipped {skipped} expired positions")

        # get all the trades for each of my positions
        # and calculate basis & validate the balances
        for contract_id, position in cls.contract_positions.items():
            pos_id = position["id"]
            trades = ledgerx.Positions.list_all_trades(pos_id)
            logging.info(f"got {len(trades)} trades for {contract_id} {position}")
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
            #logging.info(f"final pos {pos} basis {basis} position {position}")
            if position["type"] == "short":
                assert(pos <= 0)
            else:
                assert(position["type"] == "long")
                assert(pos >= 0)
            assert(pos == position['size'])
            position["basis"] = basis
            contract_label = cls.all_contracts[contract_id]["label"]

            logging.info(f"Position after trades {contract_id} {contract_label} {position}")
            
        if not skip_expired:
            # zero out expired positions -- they no longer exist
            for contract_id, expired in cls.expired_contracts.items():
                if contract_id in cls.contract_positions:
                    position = cls.contract_positions[contract_id]
                    position['expired_size'] = position['size']
                    position['size'] = 0
                    logging.info(f"Adjusted expired position {position}")

        
