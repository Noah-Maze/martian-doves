from md_worker import SimpleWorker
from work_source import SimpleSource, FileSource
from os import environ

# # Simple Worker
# my_worker = SimpleWorker(SimpleSource())
# my_worker.start()

# File-based Worker
# Use an environment variable for file source so local and docker testing are easy
state_file_source = environ.get('MD_SOURCE', 'simpleStates')
my_worker = SimpleWorker(FileSource(state_file_source))
my_worker.start()
