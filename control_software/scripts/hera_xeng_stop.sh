#! /bin/bash

# source conda environment
source ~/hera-venv/bin/activate hera

hera_catcher_stop_data.py hera-sn1
hera_ctl.py stop
hera_catcher_down.sh
xtor_down.sh
