# EGESS
Experimental Gear for Evaluation of Swarm Systems (Check-In Model for SilenceSniffer)

## Comments
Since this is a schientific tool with a lot of intricacies intended to be widely disseminated and replicated, it uses the *comment-everything* style. Each time you change something, you must also update all comments that may be affected by the change.

## Docstrings
This project uses *Google style* docstrings. Although this is a scientific project, we don't use NumPy style because it is too bulky.

## How to run it?

### Step I: Start the nodes
```
./start_nodes.sh
```

### Step II: Observe the logs in real time
Possibly in another terminal (but in the same directory):
```
tail -f run.log
```
This will likely be producing a lot of telemetry. `Ctrl+C` stops it.

### Step III: Send a trigger message
If needed, send a trigger message to initiate the forwarding sequence. You can repeat it as needed. For example, if you want to send the trigger message to port 9002 (the third node), then the command will be as follows:

```
python3 trigger.py 9002 trigger_msg.json
```

### Step IV: Stop the nodes
Stop the network and kill all nodes using this command:
```
./stop_nodes.sh
```

### Step V: Observe the logs and the data
After running, the CSV-formatted data is in `data.csv` and the log of all the output is in `run.log`. Please note that the data will be backed up in `backupdata` next time you run `start_nodes.sh`. The message log, however, will be erased upon next run of `start_nodes.sh`. Logs, data and backups of data will be ignored by Git.

## A note about logging messages
All the messages to accrue `run.log` should be done with **a single argument**. In other words, the `print()` function must not use any commas to separate logged fields; use `format()` instead. Otherwise, the separate arguments of `print()` might be logged in separate spots in the log. Also, it is important to finish each string with a newline, even though `print()` adds a newline to the output. For example, if you want to log the message `foo`, you must do it like this: `print("foo\n")`.

### Step VI: Load the Gif and PNG for current run
When a run is complete, the gif and png created by visuals.py may not necessarily load or may only show the visuals of the previous run. To solve this, you must cd into the area of your file where data.csv is located. Most likely this will just be the name of project (in my case Egess Second Attempt). Afterwards, simply load the visuals.py file and they should appear.
```
cd "Egess Second Attempt"   # or wherever data.csv lives
python3 visuals.py
```
