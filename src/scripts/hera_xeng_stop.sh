#! /bin/bash
ource ~/.bashrc
set -e # exit with an error if any subcommand returns an error; .bashrc has such a command!

hera_catcher_stop_data.py hera-sn1
hera_ctl.py stop
hera_catcher_down.sh
xtor_down.sh
