from md_worker import OneAtATimeWorker
from work_source import TemporalSemaphore, SharedFileSource
from os import environ

# each worker needs a unique name (we get this for free with docker-compose HOSTNAME)
worker_name = environ.get('HOSTNAME', 'md-worker')

state_file_source = environ.get('MD_SOURCE', 'interestingStates')
semaphore = TemporalSemaphore(assignment_phase_length=3, commitment_phase_length=3, update_phase_length=3)
my_worker = OneAtATimeWorker(worker_name, SharedFileSource(state_file_source, semaphore))
my_worker.start()
