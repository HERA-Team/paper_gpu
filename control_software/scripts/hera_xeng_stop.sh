#! /bin/bash

# set PATH explicitly
export PATH="/usr/local/bin:/usr/bin:/bin"

# source conda environment
source ~/hera-venv/bin/activate hera

hera_catcher_stop_data.py hera-sn1
hera_ctl.py stop
hera_catcher_down.sh
xtor_down.sh
