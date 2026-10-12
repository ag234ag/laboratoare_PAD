from clients import BrokerApi, new_id


def test_transient_state_is_lost_on_restart_and_no_file_is_written(transient_broker, tmp_path):
    with BrokerApi(transient_broker.address) as api:
        api.subscribe("sub-t", "sport")
        api.publish(new_id(), "sport", "goal")
        state = api.state()
        assert state.storage_mode == "transient"
        assert len(state.messages) == 1

    transient_broker.restart()

    with BrokerApi(transient_broker.address) as api:
        state = api.state()
        assert len(state.messages) == 0
        assert len(state.subscriptions) == 0
    assert not (tmp_path / "broker.db").exists()
