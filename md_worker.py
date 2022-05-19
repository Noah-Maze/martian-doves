import time

class SimpleWorker(object):
    def __init__(self, name, work_source):
        print(f"Creating a SimpleWorker with a {work_source} at {time.time()%10000:2f}")
        self.work_source = work_source
        self.worker_name = name

    def start(self):
        self.work_source.register_worker(self.worker_name)
        while self.work_source.has_work():
            print(f"Retrieving initial work from {self.work_source}...")
            # Get Work shall block until work is available
            machine = self.work_source.get_work(self.worker_name)
            if machine is None:
                # We have more workers than work so this one can go down
                break
            self.work_source.commit_to_work(self.worker_name, machine.name)
            originating_state_name = machine.name
            while machine:
                ''' Note to self: Multiple options here!

                    Worker could be greedy and work on this until a terminal
                    state is reached, or it could yield it back to the state
                    source.  Since the spec is 1-to-1 (one out-state per
                    in-state) lets be greedy for now.

                    Another note: In the face of heterogeneous workers, some
                    states might not be completeable by the worker that
                    originated it.
                '''
                machine = machine.tick()
            self.work_source.save_result(self.worker_name, originating_state_name, None)
        print("No more work to do!\n\n")

class OneAtATimeWorker(object):
    def __init__(self, name, work_source):
        print(f"Creating a OneAtATimeWorker with a {work_source} at {time.time()%10000:.2f}")
        self.work_source = work_source
        self.worker_name = name

    def start(self):
        self.work_source.register_worker(self.worker_name)
        while self.work_source.has_work():
            # Get work
            print(f"Retrieving work from {self.work_source}...")
            machine = self.work_source.get_work(self.worker_name)
            if machine is None:
                # We have more workers than work so this one can go down
                break
            # Commit to work
            self.work_source.commit_to_work(self.worker_name, machine.name)
            # Perform work
            originating_state_name = machine.name
            machine = machine.tick()
            # Save the result
            self.work_source.save_result(self.worker_name, originating_state_name, machine)
        print("No more work to do!\n\n")
