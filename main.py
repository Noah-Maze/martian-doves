from md_worker import SimpleWorker, OneAtATimeWorker
from work_source import TemporalSemaphore, SharedFileSource, CommunicativeFileSource, FileSource, SimpleSource
from os import environ

# each worker needs a unique name (we get this for free with docker-compose HOSTNAME)
worker_name = environ.get('HOSTNAME', 'md-worker')
# Use an environment variable for file source so local and docker testing are easy
state_file_source = environ.get('MD_SOURCE', 'simpleStates')

# # Simple Worker
# my_worker = SimpleWorker(worker_name, SimpleSource())
# my_worker.start()

# File-based Worker
# my_worker = SimpleWorker(worker_name, FileSource(state_file_source))
# my_worker.start()

# File-based Worker with communication over file system
# my_worker = SimpleWorker(worker_name, CommunicativeFileSource(state_file_source))
# my_worker.start()

# Test non-greedy worker
# my_worker = OneAtATimeWorker(worker_name, FileSource(state_file_source))
# my_worker.start()

# Final answer!
semaphore = TemporalSemaphore(assignment_phase_length=3, commitment_phase_length=3, update_phase_length=3)
my_worker = OneAtATimeWorker(worker_name, SharedFileSource(state_file_source, semaphore))
my_worker.start()
