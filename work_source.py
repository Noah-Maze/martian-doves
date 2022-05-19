import pathlib
import time
import json
import os
from collections import namedtuple

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
        self.message = f"I am saying {target}"
    def __str__(self):
        return type(self).__name__ + f"({self.target})"
    def tick(self):
        print(self.message)
        time.sleep(1)
        # No follow up work (one, terminal state)
        return None


machines = {
    'simple': lambda name, payload: SimpleMachine.FromJson(name, payload)
}

def fetch_state(subdirectory, name):
    machine_state = None
    state_path = f"{subdirectory}/{name}"
    # get the contents
    with open(state_path) as f:
        m_state = json.load(f)
    # convert object to a machine using a lambda chosen by the state name
    state_name = m_state['state']
    if state_name in machines:
        # use existing lookup dict to turn dict into an object
        machine_state = machines[state_name](name, m_state['payload'])
        print(f"Loaded {state_path} and added a {machine_state}.")
    else:
        print(f"Warning!!! {state_path} contains an invalid state '{machine_state}'.")
    return machine_state

def try_create_directory(path):
    try:
        # Unfortunately this block is subject to a race condition
        # so we need to catch the exception for the case where
        # another node created it after we looked for it.
        if not os.path.exists(path):
            os.mkdir(path)
    except FileExistsError:
        # As long as /somebody/ creates it, we don't care.
        pass

def remove_flag(path):
    # Remove in_progress flag
    try:
        os.remove(path)
    except Exception as e:
        print(f"Couldn't remove progress file at {path} (got {e})")

def create_flag(path):
    # Touch complete flag
    with open(path,'a') as _:
        pass

class SimpleSource(object):
    def __init__(self):
        self.work = [ SimpleMachine("Machine 1", "Goodbye!"), SimpleMachine("Machine 2", "Hello.") ]
        print(f"Initialized {self} with {len(self.work)} machine states.")
        def __str__(self):
            return type(self).__name__
        time.sleep(1)
    def __str__(self):
        return type(self).__name__
    def get_work(self, worker_name=None):
        print(f"Delegating work, ({len(self.work)-1} jobs remaining).")
        time.sleep(1)
        return self.work.pop()
    def has_work(self):
        return len(self.work)>0
    def save_result(self, worker_name, originating_state_name, new_state):
        if new_state:
            self.work.append(new_state)
    # For backwards compatibility
    def register_worker(self, worker_name):
        pass
    def commit_to_work(self, worker_name, state_machine_name):
        pass

class FileSource(object):
    def __init__(self, subdirectory):
        self.work = []
        # List files in subdirectory
        state_files = list(pathlib.Path(subdirectory).glob('*.state*'))
        for s_file in state_files:
            # get the filename
            name = s_file.parts[-1]
            machine_state = fetch_state(subdirectory, name)
            if machine_state:
                self.work.append(machine_state)
        print(f"Initialized {self} with {len(self.work)} machine states from {subdirectory}.")
        time.sleep(1)
    def __str__(self):
        return type(self).__name__
    def get_work(self, worker_name=None):
        print(f"Delegating work, ({len(self.work)-1} jobs remaining).")
        time.sleep(1)
        return self.work.pop()
    def has_work(self):
        return len(self.work)>0
    def save_result(self, worker_name, originating_state_name, new_state):
        if new_state:
            self.work.append(new_state)
    # For backwards compatibility
    def register_worker(self, worker_name):
        pass
    def commit_to_work(self, worker_name, state_machine_name):
        pass


class CommunicativeFileSource(FileSource):
    ''' A file source with some writing to test out volume-based state sharing.
        Not consistent at all, just a test!

        Note: Inherits from FileSource
    '''
    def __init__(self, subdirectory):
        super().__init__(subdirectory)
        self.worker_path = subdirectory + '/workers'
        try_create_directory(self.worker_path)

    def get_work(self, worker_name=None):
        if worker_name:
            # Touch worker file
            with open(f"{self.worker_path}/{worker_name}",'a') as _:
                pass
        # Generate list of workers
        worker_files = list(pathlib.Path(f"{self.worker_path}/").glob('*'))
        # Trim path down to worker name
        workers = [wf.parts[-1] for wf in worker_files]
        print(f"Delegating work, ({len(self.work)-1} jobs remaining, workers: {workers}).")
        time.sleep(1)
        return self.work.pop()
    def has_work(self):
        return len(self.work)>0


Phase = namedtuple("Phase", "name start stop")
class Phase(object):
    def __init__(self, name, start, stop, cycle_time):
        self.name = name
        self.start = start
        self.stop = stop
        self.cycle_time = cycle_time
    def is_active(self, cycle_t_now):
        # Half-open timespan e.g. Phase('foo',1,5) Starts at 1 and ends BEFORE 5
        return (cycle_t_now >= self.start) and (cycle_t_now < self.stop)
    def wait_until_active(self, cycle_t_now):
        if self.is_active(cycle_t_now):
            return
        sleep_time = self.start - cycle_t_now
        if sleep_time < 0:
            sleep_time += self.cycle_time
        print(f"Waiting until {self.name}... ({sleep_time:.2f} seconds remaining)")
        time.sleep(sleep_time)

class TemporalSemaphore(object):
    ''' Contains useful utilities for configuring and interacting with UTC
        time-based coordination

        States:
            1. Assignment phase at t_0
            2. Commitment phase at t_0 + assignment_phase_length + buffer_seconds
            3. Update phase at t_0 + assignment_phase_length + buffer_seconds + commitment_phase_length + buffer_seconds
            4. Assignment phase at t_cycle_time
            5. etc

        Each phase ends in a buffer zone because python file interactions are
        not ACID compliant and delays could cause writes to spill into forbidden
        phases.
    '''
    def __init__(self, assignment_phase_length, commitment_phase_length, update_phase_length, buffer_seconds=1):
        self.cycle_time = sum([assignment_phase_length, commitment_phase_length, update_phase_length]) + 3 * buffer_seconds
        self.phases = {
            "Worker Assignment": Phase("Worker Assignment",
                                        start = 0,
                                        stop =  assignment_phase_length,
                                        cycle_time = self.cycle_time
                                        ),
            "Worker Commitment": Phase("Worker Commitment",
                                        start = assignment_phase_length + buffer_seconds,
                                        stop = assignment_phase_length + buffer_seconds + commitment_phase_length,
                                        cycle_time = self.cycle_time
                                        ),
            "Update System": Phase("Update System",
                                        start = assignment_phase_length + buffer_seconds + commitment_phase_length + buffer_seconds,
                                        stop = self.cycle_time - buffer_seconds,
                                        cycle_time = self.cycle_time
                                        )
            }
        self.assignment_phase_length = assignment_phase_length
        self.commitment_phase_length = commitment_phase_length
        self.update_phase_length = update_phase_length
        self.buffer_seconds = buffer_seconds
    def current_cycle_time(self):
        # utc seconds since epoch modulo cycle time results in the time for this cycle
        current_time = time.time()
        return current_time % self.cycle_time
    def current_phase(self):
        for phase_name in self.phases:
            if self.phases[phase_name].is_active(self.current_cycle_time()):
                return phase_name
        return "Buffer"
    def wait_until(self, phase):
        now = self.current_cycle_time()
        print(f"Waiting for {phase}, current phase is {self.current_phase()} (cycle time: {now:.2f})")
        self.phases[phase].wait_until_active(now)

def simple_work_assignment(workers, states):
    ''' Pair workers off with states in alphabetical order.

        Extremely naive approach that results in:
          * Worker skew - a worker called "aaaaa" will have a much higher
            workload than worker "zzzz" if there's not enough work for all
            nodes
          * State skew - a state called "zzzzz" will be worked on last if there
            are more states than workers.

        TODO: This can be easily fixed by sorting by a hash salted with the hash of the system state.
    '''
    # Create tuples of worker, state pairs (truncates the longer of the two)
    assignments = zip(sorted(workers), sorted(states))
    # convert it to a dictionary for ease of use
    return dict(assignments)

def increment_state_name(state_name):
    last_hyphen_location = state_name.rfind('-')
    if last_hyphen_location >= 0:
        # this might already have an incremented suffix, or it might just have a
        # hyphen in the name
        try:
            new_increment = int(state_name[last_hyphen_location+1:])
            return state_name[:last_hyphen_location] + f"-{new_increment}"
        except ValueError:
            # Couldn't convert the rest of the string to an int
            # so the hyphen probably wasnt part of an increment
            # No problem!  Just add a new increment on the end
            # as per usual
            pass
    return f"{state_name}-1"

class SharedFileSource(object):
    ''' Finally the real state source!

        This source assigns work to the worker just like the rest, but it is
        time-constrained to allow for distributed consensus-building.  The file
        system is the sole source of communication between workers, and
        consistency goals are satisfied by delaying state and worker updates
        when worker assignment is underway.

        Warning: A Malicious state filename combination e.g. "somename.state-2"
        and "somename.state" could lead to the results of the second clobbering
        the first.
    '''
    def __init__(self, subdirectory, semaphore):
        self.subdirectory = subdirectory
        self.semaphore = semaphore
        # Create directories for workers, progressing, and done states
        try_create_directory(subdirectory + '/workers')
        try_create_directory(subdirectory + '/in_progress')
        try_create_directory(subdirectory + '/done')

    def __str__(self):
        return type(self).__name__

    def register_worker(self, worker_name):
        self.semaphore.wait_until("Update System")
        self._set_worker(worker_name, busy=False)

    def get_work(self, worker_name):
        self.semaphore.wait_until("Worker Assignment")
        states = self._get_states()
        available_workers = self._get_workers()
        # Deterministically order states and workers so all nodes agree
        # on which worker works wchich states
        chosen_state_name = simple_work_assignment(available_workers, states).get(worker_name, None)
        if chosen_state_name is None:
            # Too many workers, too little states
            return None
        return fetch_state(self.subdirectory, chosen_state_name)

    def commit_to_work(self, worker_name, state_machine_name):
        self.semaphore.wait_until("Worker Commitment")
        self._set_worker(worker_name, busy=True)
        self._set_state(state_machine_name, in_progress=True)

    def save_result(self, worker_name, originating_state_name, new_state):
        self.semaphore.wait_until("Update System")
        # Add a number to the next state name for debugging
        if new_state:
            self._write_new_state(increment_state_name(originating_state_name), new_state)
        self._set_state(originating_state_name, done=True)
        self._set_worker(worker_name, busy=False)

    def has_work(self):
        return len(self._get_states())>0

    ## Quasi private methods to work with the file system
    def _get_states(self):
        state_names = set([ state_path.parts[-1] for state_path in pathlib.Path(self.subdirectory).glob('*.state*')])
        # Get the names of the in progress and completed states
        in_progress_states = set([ state_path.parts[-1] for state_path in pathlib.Path(self.subdirectory+'/in_progress').glob('*.state*')])
        completed_states =   set([ state_path.parts[-1] for state_path in pathlib.Path(self.subdirectory+'/done').glob('*.state*')])
        unavailable_states = in_progress_states.union(completed_states)
        # Available states are niether in progress nor completed (Converted to a list for easy sorting)
        available_state_names = list(state_names - unavailable_states)
        print(f"Found {len(available_state_names)} available machine states in {self.subdirectory}.")
        return available_state_names
    def _get_workers(self):
        available_workers = set([ worker_path.parts[-1] for worker_path in pathlib.Path(self.subdirectory+'/workers').glob('*')])
        return available_workers
    def _write_new_state(self, name, state):
        with open(f"{self.subdirectory}/{name}") as f:
            json.dump(state.ToJson(), f)
    def _set_state(self, state_name, done=False, in_progress=False):
        if in_progress:
            # Touch in_progress flag
            create_flag(f"{self.subdirectory}/in_progress/{state_name}")
        if done:
            # Touch complete flag
            create_flag(f"{self.subdirectory}/done/{state_name}")
        if not done:
            remove_flag(f"{self.subdirectory}/done/{state_name}")
        if not in_progress:
            remove_flag(f"{self.subdirectory}/in_progress/{state_name}")
    def _set_worker(self, name, busy):
        if busy:
            remove_flag(f"{self.subdirectory}/workers/{name}")
        if not busy:
            # Touch complete flag
            create_flag(f"{self.subdirectory}/workers/{name}")
