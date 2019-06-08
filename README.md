# ecloud

In a nutshell:

* the *boss* orchestrates the *worker* VMs
* a task queue is stored in a database
* results are stored on the *datastore*
* tasks are stored in a queue and are run in parallel
* tasks may depend on the results of other tasks

The boss, workers and datastore may be the same machine or any number of different virtual machines in a network.

ecloud is a set of python scripts that automates deployment of virtual machines and handing out tasks. It's hacky, quick and dirty runs into race conditions and overloads the network as soon as too many tasks finish at once. Nevertheless, it's robust enough to save an enormous amount of time.

All VMs communicate with a central VM (the "boss"), which instead of running a server contains a set of scripts that manipulate a database. Workers call these scripts over SSH. The database encodes the state of the whole system and is the only piece of persistent state in the system. Workers are created by a script that can be run from the boss or from elsewhere. Upon initialisation, worker VMs are told where the boss can be found on the network. The worker VMs are configured to run a script upon startup (after waiting for a fixed amount of time in the hope that the network will be up by then). The worker script will query the boss for a task. Having been dealt a task, the worker will try and execute it, record the results, send them to the datastore, and query the boss for another task. This process will repeat until the worker doesn't receive a new task from the boss at which point it will request the boss to be terminated and quietely await its inevitable fate.

A task consists of four sets of commands:

* downloading required software and data from a central datastore,  
* running the task
* uploading the results to the datastore
* cleaning up

`pop-task.py`, `hello.py`, `completed-task.py` are a poor man's server endpoints that are called by workers over ssh.

# Setup

Worker template need BOSS public key
Worker template need DATAstore public key

Set BOSS contextualization to use your public key
Login to boss
Check database
```python admin.py status```
Check boss and datastore address
```python admin.py show_context```
Boss address can be set automatically, provided that the right boss template is configured in settings.py.
However, for that to happen, boss_address needs to not be present in the Context table.
So
```python admin.py drop_context```
Set the datastore address
```python admin.py set_datastore <datastore_ip>```

The flow for creating and running tasks is as follows:

1. Create a list of tasks locally and upload to boss
2. Load the tasks with push-tasks.py

```
python push-tasks.py <tasks-file.json>
```

3. Instantiate workers 

```
python admin.py instantiate_worker [<num-workers>]
```

4. Wait for workers to come online. If this happens, show_workers will show their IP.

```
python admin.py instantiate_worker [<num-workers>]
```

If this doesn't happen soon after opennebula reports the worker to be running, investigate by logging into the worker from Boss, and run `./debug.sh`, which will run the worker's startup script while printing every command.

6. Monitor progress with status




# Settings.py

Configuring datastore and worker templates

# The database:

Tasks, Workers, Context

## Context:

datastore
boss_address
(automatically set throught the ONE api)
