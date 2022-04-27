#! /bin/bash

hosts=`ssh hera-snap-head cat /etc/hosts`

nodes=`sudo tcpdump -n -e -i eth3 -c 4000 udp | python -c "import sys; lines = sys.stdin.read().splitlines(); s = ['.'.join(L.split()[9].split('.')[:4]) for L in lines]; print('\n'.join(s))" | sort | uniq`

for node in $nodes
do
python -c "host = [' '.join(L.split()[::-1]) for L in '''${hosts}'''.splitlines() if L.split()[:1] == ['${node}']]; host.sort() ; print(host[0])"
#echo $hosts | grep $node
done

