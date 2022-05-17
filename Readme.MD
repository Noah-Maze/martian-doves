# (draft)  

## Worker allocation State Diagram  
Synchronize by the clock with a configurable time step
Workers clock in in a given allocation period
Work is algorithmically assigned based on available states and available workers in a way that is consistent for all nodes


## Example Machines  
SimpleMachine - Terminal.  Prints a message.


## Docker Notes  
Run arbitrary number of workers: `docker compose up --scale worker=5`

# TODO
[X] Most basic implementation possible of Worker, Source, and Machine
[X] Source to read from disk
[X] Read from a shared dockerized volume (no consistency, tons of repeated work) with DiskSource
[ ] Verify that workers can alter disk and changes percolate 
[ ] Consistent source reads from disk
[ ] More interesting states
[ ] Production logs instead of embarrassing print debug statements
