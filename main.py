from md_worker import Worker
from work_source import SimpleSource

my_worker = Worker(SimpleSource())
my_worker.start()
