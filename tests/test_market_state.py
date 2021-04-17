import ledgerx


def test_methods():
    class_methods = dir(ledgerx.MarketState)
    assert "clear" in class_methods
    assert "cost_to_close" in class_methods
    assert "bid" in class_methods
    assert "ask" in class_methods
    assert "fee" in class_methods
    assert "is_same_option_date" in class_methods
    assert "contract_is_expired" in class_methods
    assert "is_qualified_covered_call" in class_methods
    assert "is_my_order" in class_methods
    assert "handle_order" in class_methods
    assert "get_top_from_book_state" in class_methods
    assert "handle_book_state" in class_methods
    assert "load_books" in class_methods
    assert "get_top_book_states" in class_methods
    assert "get_next_day_swap" in class_methods
    assert "to_contract_label" in class_methods
    assert "contract_added_action" in class_methods
    assert "contract_removed_action" in class_methods
    assert "trade_busted_action" in class_methods
    assert "open_positions_action" in class_methods
    assert "collateral_balance_action" in class_methods
    assert "book_top_action" in class_methods
    assert "heartbeat_action" in class_methods
    assert "action_report_action" in class_methods
    assert "handle_action" in class_methods
    assert "retrieve_contract" in class_methods
    assert "update_basis" in class_methods
    assert "update_all_positions" in class_methods
    assert "update_position" in class_methods
    assert "load_market" in class_methods
    assert "start_websocket_and_run" in class_methods

