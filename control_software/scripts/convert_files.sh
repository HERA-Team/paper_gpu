#!/bin/bash

# This command runs in cron to convert files taken from the previous night.
# Usage:
#   convert_files.sh
# It figures out the current JD, moves into that directory, and then converts
# files. It also adds them to M&C and the RTP list of files to process.

source ~/.bashrc

logfile=$HOME/logs/convert_files.$(date +%Y%m)
print_diagnostics=false

if conda activate file_conversion >>$logfile 2>&1 ; then
    : # everything OK
else
    ec=$?
    echo >$2 "error: had a problem activating file_conversion environment."
    print_diagnostics=true
fi

function inner() {
    echo -n "starting file conversion: "
    date

    today=$(python -c "from astropy.time import Time; t0 = Time.now(); print(int(t0.jd))")
    cd /data/$today

    # loop over files and convert
    for fn in `ls *.meta.uvh5`; do
        sum_fn_in="${meta_fn/meta.hdf5/sum.dat}"
        diff_fn_in="${meta_fn/meta.hdf5/diff.dat}"
        sum_fn_out="${sum_fn_in/dat/uvh5}"
        diff_fn_out="${diff_fn_in/dat/uvh5}"
        sum_cmd="taskset 0xfc0 OMP_NUM_THREADS=6 hera_convert_uvh5.py -i $sum_fn_in -m $meta_fn -o $sum_fn_out"
        diff_cmd="taskset 0xfc0 OMP_NUM_THREADS=6 hera_convert_uvh5.py -i $diff_fn_in -m $meta_fn -o $diff_fn_out"
        echo $sum_cmd
        eval $sum_cmd
        echo $diff_cmd
        eval $diff_cmd

        # add to M&C
        cmd="mc_add_observation.py $sum_fn_out"
        echo $cmd
        eval $cmd
        # add to RTP
        cmd="mc_rtp_launch_record.py $sum_fn_out"
        echo $cmd
        eval $cmd
    done
}

if inner >>$logfile 2>&1 ; then
    ec=$? # everything OK
else
    ec=$?
    echo >&2 "error; file conversion failed"
    print_diagnostics=true
fi

if $print_diagnostics ; then
    echo >&2 "Exit code was $ec"
    echo >&2 "Log file is $logfile"
    echo >&2 "Most recent log output is:"
    echo >&2
    tail -n40 $logfile >&2
else
    date >>$logfile
    echo "Success." >>$logfile
fi

exit $ec
