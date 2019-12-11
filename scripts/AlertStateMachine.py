
import pickle
class AlertLevel:
    NO_ALERT = 0
    LOW_ALERT = 1
    MEDIUM_ALERT = 2
    HI_ALERT = 3


class  AlertState:
    def __init__(self, state_id, state):
        self._state_id = state_id
        self._current_state = state
    @property
    def state(self):
        return self._current_state
    @state.setter
    def state(self, state):
        if state != self._current_state:
            self._current_state = state
            return True
        return False
    def __str__(self):
        ret_val = "No Alert"
        if self._current_state == AlertLevel.LOW_ALERT:
            ret_val = "Low Alert"
        elif self._current_state == AlertLevel.MEDIUM_ALERT:
            ret_val = "Medium Alert"
        elif self._current_state == AlertLevel.HI_ALERT:
            ret_val = "High Alert"

        return ret_val

class NoAlertState(AlertState):
    def __init__(self, id):
        AlertState.__init__(self, id, AlertLevel.NO_ALERT)
class LowAlertState(AlertState):
    def __init__(self, id):
        AlertState.__init__(self, id, AlertLevel.LOW_ALERT)
class MedAlertState(AlertState):
    def __init__(self, id):
        AlertState.__init__(self, id, AlertLevel.MEDIUM_ALERT)
class HiAlertState(AlertState):
    def __init__(self, id):
        AlertState.__init__(self, id, AlertLevel.HI_ALERT)


class AlertStateMachine:
    def __init__(self):
        self._states = []
        return

    def get_state(self, id):
        return

    def update_state(self, id):
        return False


class SiteStates(AlertStateMachine):
    def __init__(self, sites):
        self._site_state = {}

    def set_sites(self, sites):
        for site_id in sites:
            self._site_state[site_id] = NoAlertState(site_id)

    def get_state(self, site_id):
        if site_id not in self._site_state:
            self._site_state[site_id] = NoAlertState(site_id)
        site = self._site_state[site_id]
        return site

    def update_state(self, site_id, state):

        if site_id not in self._site_state:
            self._site_state[site_id] = NoAlertState(site_id)

        site = self._site_state[site_id]
        if site.state != state.state:
            self._site_state[site_id] = state
            return True

        return False
