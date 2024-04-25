#! /bin/bash

# source conda environment
source ~/miniforge3/bin/activate hera

hera_catcher_ctl.py stop
hera_catcher_down.sh
xtor_down.sh
