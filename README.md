# ECLOUD - distribute computation over machines connected to a network

I wrote this in the spring of 2019 to distribute tasks over a large number of VMs. It's basically a task queue that communicates with VMs over MQTT (don't ask).
It keeps track of which tasks are finished and schedules tasks based on whether its dependencies are met.
It also orchestrates moving data (simulation training data to the VMs and simulation results to virtual volumes) over the network.

This is a fairly hacky and ad-hoc solution that you should probably not try to use.
There are far better existing solutions addressing this type of problem that I was blissfully unaware of.

Nevertheless, I've used this tool to distribute simulations over 255 VMs (here I ran out of IP addresses) picking tasks (with inter-dependencies) off the task queue in parallel.
As long as the tasks take sufficiently long (about a minute) to execute, it could handle this load without everything blowing up.
It caused the computing resources provider to send me a worried email.

# Setup

Worker template need BOSS public key
Worker template need DATAstore public key

Set BOSS contextualization to use your public key
Login to boss
Install python3.7 git mysql mosqitto
Setup mysql and add database
Configure settings.py
Create virtualenv
Open port 1883 in firewall
```sudo ufw allow 1883```

Check what's in the database
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

