import time
import json

class SimpleMachine(object):
    @classmethod
    def FromJson(cls, name, payload):
        return cls(name, payload['target'])
    def ToJson(self):
        state = {
            "state": "simple",
            "payload": {
                "target": self.target
            }
        }
        return json.dumps(state)
    def __init__(self, name, target):
        self.name = name
        self.target = target
        self.message = f" * From SimpleMachine: I am saying {target}"
    def __str__(self):
        return type(self).__name__ + f"({self.target})"
    def tick(self):
        print(self.message)
        time.sleep(1)
        # No follow up work (one, terminal state)
        return None

class CountdownMachine(object):
    @classmethod
    def FromJson(cls, name, payload):
        return cls(name, payload['count'])
    def ToJson(self):
        state = {
            "state": "countdown",
            "payload": {
                "count": self.count
            }
        }
        return json.dumps(state)
    def __init__(self, name, count):
        self.name = name
        self.count = count
    def __str__(self):
        return type(self).__name__ + f"({self.count})"
    def tick(self):
        self.count -= 1
        print(f" * From CountdownMachine: Counting down! {self.count} remaining.")
        time.sleep(1)
        if self.count>0:
            return CountdownMachine(self.name, self.count)
        return None

class SlowCountdownMachine(CountdownMachine):
    ''' Intentionally run longer than the cycle_time to verify that
        workers can miss an Assignment phase without incident
    '''
    @classmethod
    def FromJson(cls, name, payload):
        return cls(name, payload['count'])
    def ToJson(self):
        state = {
            "state": "slow-countdown",
            "payload": {
                "count": self.count
            }
        }
        return json.dumps(state)
    def __init__(self, name, count):
        self.name = name
        self.count = count
    def __str__(self):
        return type(self).__name__ + f"({self.count})"
    def tick(self):
        self.count -= 1
        print(f" * From SlowCountdownMachine: Counting down! {self.count} remaining.")
        time.sleep(10)
        if self.count>0:
            return SlowCountdownMachine(self.name, self.count)
        return None
