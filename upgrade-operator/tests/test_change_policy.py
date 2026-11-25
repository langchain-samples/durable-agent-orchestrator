from middleware.change_policy import _violation


def test_approval_needs_change_ticket():
    assert _violation("decide_approval_gate", {"approved": True, "comment": "looks fine"})
    assert _violation("decide_approval_gate", {"approved": True, "comment": "CHG-4711 approved by CAB"}) is None


def test_rejection_needs_no_ticket():
    assert _violation("decide_approval_gate", {"approved": False, "comment": "change freeze"}) is None


def test_early_window_needs_reason():
    assert _violation("open_maintenance_window_now", {"reason": "demo"})
    assert _violation("open_maintenance_window_now", {"reason": "lab environment used for a demo"}) is None
