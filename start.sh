#!/bin/bash
#make-run.sh
#make sure a process is always running.

export DISPLAY=:0 #needed if you are running a simple gui app.

process="AddressAPI"


cd /mnt/admin/restapi_admin && python3 update_api_usage.py &>/dev/null &
if ps ax | grep -v grep | grep $process > /dev/null
then
    exit
else
    cd /mnt/admin/restapi_admin && python3 AddressAPI.py  &>/dev/null &
fi


exit
