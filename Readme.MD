# (draft)  

## Worker allocation State Diagram  
Synchronize by the clock with a configurable time step
Workers clock in in a given allocation period
Work is algorithmically assigned based on available states and available workers in a way that is consistent for all nodes


## Example Machines  
SimpleMachine - Terminal.  Prints a message.


## Notes  
Run arbitrary number of workers: `docker compose up --scale worker=5`

Copy state files to volume:
1. Get volume Mountpoint with `docker volume inspect martian-doves_states`
2. Copy states from git repo with `sudo cp simpleStates/*.state <Mountpoint>`

View the shared volume with `docker run -it -v martian-doves_states:/stateFiles bash bash`
Clear flag-files from docker volume after testing (to re-run states).
```bash
rm /stateFiles/workers/*
rm /stateFiles/done/*
rm /stateFiles/in_progress/*

# remove later states created from initial states
rm /stateFiles/*.state-*
```

Clear local flags
```bash
rm simpleStates/workers/*
rm simpleStates/done/*
rm simpleStates/in_progress/*

# remove later states created from initial states
rm simpleStates/*.state-*
```

# TODO
[X] Most basic implementation possible of Worker, Source, and Machine
[X] Source to read from disk
[X] Read from a shared dockerized volume (no consistency, tons of repeated work) with DiskSource
[X] Verify that workers can alter disk and changes percolate
[X] Consistent source reads from disk
[ ] More interesting states
[ ] Production logs instead of embarrassing print debug statements
[ ] Refactor the interface between the semaphore and the source: more strongly-typed or merge them
