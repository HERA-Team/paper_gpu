#! /bin/bash

# source conda environment
source ~/hera-venv/bin/activate hera

hera_catcher_ctl.py endofday
hera_catcher_down.sh
xtor_down.sh
