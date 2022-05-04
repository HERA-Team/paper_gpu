#!/bin/bash

printf "\n\n\n"
echo "Starting correlator at "`date`

# source .bashrc so we have correct PATH etc.
source ~/.bashrc

# activate venv
#source ~/hera-venv/envs/hera/bin/activate
source ~/hera-venv/bin/activate hera

# define path
export PATH=$PATH:/usr/local/bin:/usr/bin:/bin

# tear down catcher (NEED TO MAKE CATCHER STABLE SO THIS IS UNNECCESARY)
hera_catcher_stop_data.py hera-sn1
hera_ctl.py stop
hera_catcher_down.sh

# Give things a chance to settle
sleep 10

# bring catcher back up without triggering data recording
hera_catcher_up.py --redislog hera-sn1
sleep 10
hera_ctl.py start

# things should now be in a state that data taking can be triggered
python ~/start_correlator.py
