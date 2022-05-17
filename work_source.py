import pathlib
import time
import json

class SimpleMachine(object):
    @classmethod
    def FromJson(cls, payload):
        return cls(payload['target'])
    def __init__(self, target):
        self.target = target
        self.message = f"I am saying {target}"
    def __str__(self):
        return type(self).__name__ + f"({self.target})"
    def tick(self):
        print(self.message)
        time.sleep(1)
        # No follow up work (one, terminal state)
        return None

machines = {
    'simple': lambda payload: SimpleMachine.FromJson(payload)
}

class SimpleSource(object):
    def __init__(self):
        self.work = [ SimpleMachine("Goodbye!"), SimpleMachine("Hello.") ]
        print(f"Initialized {self} with {len(self.work)} machine states.")
        def __str__(self):
            return type(self).__name__
        time.sleep(1)
    def get_work(self):
        print(f"Delegating work, ({len(self.work)-1} jobs remaining).")
        time.sleep(1)
        return self.work.pop()
    def has_work(self):
        return len(self.work)>0

class FileSource(object):
    def __init__(self, subdirectory):
        self.work = []
        # List files in subdirectory
        state_files = list(pathlib.Path(subdirectory).glob('*.state'))
        for s_file in state_files:
            # get the contents
            with open(s_file) as f:
                m_state = json.load(f)
            # convert object to a machine using a lambda chosen by the state name
            state_name = m_state['state']
            if state_name in machines:
                machine_state = machines[state_name](m_state['payload'])
                self.work.append(machine_state)
                print(f"Loaded {s_file} and added a {machine_state}.")
            else:
                print(f"Warning!!! {s_file} contains an invalid state '{machine_state}'.")
        print(f"Initialized {self} with {len(self.work)} machine states from {subdirectory}.")
        time.sleep(1)
    def __str__(self):
        return type(self).__name__
    def get_work(self):
        print(f"Delegating work, ({len(self.work)-1} jobs remaining).")
        time.sleep(1)
        return self.work.pop()
    def has_work(self):
        return len(self.work)>0
