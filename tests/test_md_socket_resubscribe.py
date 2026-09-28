from MarketDataSocketClient import MDSocket_io


class FakeRest:
    def __init__(self):
        self.calls = []

    def send_subscription(self, instruments, code):
        self.calls.append((code, len(instruments)))
        return {"type": "error", "description": "Instrument Already Subscribed"}


class FakeSid:
    connected = False

    def __init__(self):
        self.emits = []

    def emit(self, event, payload):
        self.emits.append((event, payload["xtsMessageCode"], len(payload["instruments"])))


def _instr(ids, seg=2):
    return [{"exchangeSegment": seg, "exchangeInstrumentID": i} for i in ids]


def test_subscriptions_are_tracked_while_disconnected_and_replayed_in_batches():
    rest = FakeRest()
    sock = MDSocket_io("tok", "user", rest_client=rest)
    sock.sid = FakeSid()

    assert sock.send_subscription(_instr(range(120)), 1512)["type"] == "error"
    sock.send_subscription(_instr([7, 8]), 1502)
    sock.send_unsubscription(_instr([8]), 1502)
    sock.track_subscription(_instr([7]), 1502)  # duplicate is ignored

    assert sock.subscription_count() == 121

    sock._resubscribe_all()

    assert sorted(rest.calls) == [(1502, 1), (1512, 20), (1512, 50), (1512, 50)]
    assert sorted(sock.sid.emits) == [
        ("join", 1502, 1), ("join", 1512, 20), ("join", 1512, 50), ("join", 1512, 50)
    ]


def test_auth_connect_error_marks_token_for_refresh():
    sock = MDSocket_io("tok", "user", token_refresher=lambda: ("new", "user2"))
    sock._on_connect_error({"message": "Invalid token"})
    assert sock._auth_error

    assert sock._refresh_token()
    assert "token=new" in sock.connection_url and "userID=user2" in sock.connection_url
