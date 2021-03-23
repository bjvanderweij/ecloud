# ECLOUD - distribute computation over machines connected to a network

I wrote this in the spring of 2019 to distribute tasks over a large number of VMs. It's basically a task queue that communicates with VMs over MQTT (don't ask).
The tasks can be arbitrary commands to be run on worker VMS.
The queue supports a basic task-dependency graph and keeps track of which tasks are finished and schedules tasks based on whether its dependencies are met.
It also orchestrates moving data (simulation training data to the VMs and simulation results to virtual volumes) over the network.

The script automatically creates and discards (kills) workers on demand.
To do this, it uses the OpenNebula API to instantiate and kill VMs on demand.

This is a fairly hacky and ad-hoc solution that you should probably not try to use.
There are far better existing solutions addressing this type of problem that I didn't have time to investigate or was blissfully unaware of.

Nevertheless, I've used this tool to distribute simulations over 255 VMs (here I ran out of IP addresses) picking tasks (with inter-dependencies) off the task queue in parallel and generating gigabytes of data.
As long as the tasks take sufficiently long (about a minute) to execute, it could handle this load without everything blowing up.
It caused the computing resources provider to send me a worried email.

# Setup

**These instructions are not very complete or explicit; they're mostly for personal reference.**
To make this work, a of couple of things need to be set up.

First of all, three VM templates should be created: one for workers, one for "the boss", and one for the datastore.

The workers need to have the boss' public key in their authorised SSH keys as well as the datastores' public key.

Seting up the boss:

* Add your own public key to authorised keys of the boss.
* Install python3.7 git mysql mosqitto
* Setup mysql and add database
* Configure settings.py
* Create virtualenv
* Open port 1883 in firewall: ```sudo ufw allow 1883```

## Some admin commands

Check what's in the database

```bash
$ python admin.py status
```

Check boss and datastore address

```bash
$ python admin.py show_context
```

The boss address can be set automatically, provided that the right boss template is configured in settings.py.
However, for that to happen, boss_address needs to not be present in the Context table.

So:

```bash
$ python admin.py drop_context
```

Set the datastore address

```bash
$ python admin.py set_datastore <datastore_ip>
```

# Scheduling and running tasks

The steps involved in scheduling and running tasks are:

1. Create a list of tasks locally and upload to boss
2. Load the tasks with push-tasks.py

```bash
$ python push-tasks.py <tasks-file.json>
```

3. Instantiate workers 

```bash
$ python admin.py instantiate_worker [<num-workers>]
```

4. Wait for workers to report for duty. When this happens, show_workers will show their IP.

```bash
$ python admin.py show_workers
```

If this doesn't happen soon after OpenNebula reports the worker to be running, investigate by logging into the worker from Boss, and run `./debug.sh`, which will run the worker's startup script while printing every command.

5. Monitor progress with status

