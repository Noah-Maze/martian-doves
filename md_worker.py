

class Worker(object):
    def __init__(self, work_source):
        self.work_source = work_source

    def start(self):
        while self.work_source.has_work():
            print(f"Retrieving initial work from {self.work_source}...")
            machine = self.work_source.get_work()
            while machine:
                ''' Note to self: Multiple options here!

                    Worker could be greedy and work on this until a terminal
                    state is reached, or it could yield it back to the state
                    source.  Since the spec is 1-to-1 (one out-state per
                    in-state) lets be greedy for now.
                '''
                machine = machine.tick()
        print("No more work to do!")
