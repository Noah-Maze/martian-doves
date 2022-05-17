import time

class SimpleSource(object):
    def __init__(self):
        self.work = [ SimpleMachine("Goodbye!"), SimpleMachine("Hello.") ]
        print(f"Initialized {self} with {len(self.work)} machine states.")
        time.sleep(1)
    def __str__(self):
        return type(self).__name__
    def get_work(self):
        print(f"Delegating work, ({len(self.work)-1} jobs remaining).")
        time.sleep(1)
        return self.work.pop()
    def has_work(self):
        return len(self.work)>0

class SimpleMachine(object):
    def __init__(self, target):
        self.message = f"I am saying {target}"
    def tick(self):
        print(self.message)
        time.sleep(1)
        # No follow up work (one, terminal state)
        return None
