#!/bin/bash
#make-run.sh
#make sure a process is always running.

export DISPLAY=:0 #needed if you are running a simple gui app.

cd /mnt/admin/restapi_admin && python3 update_api_usage.py &>/dev/null &

process="residentialapi"

if ps ax | grep -v grep | grep $process > /dev/null
then
    printf "Residential API is running"
else
    cd /mnt/admin/restapi_admin && python3 residentialapi.py  &>/dev/null &
    printf "Residential API started"
fi


process="AddressAPI"

if ps ax | grep -v grep | grep $process > /dev/null
then
    printf "Residential Address API is running"
else
    cd /mnt/admin/restapi_admin && python3 AddressAPI.py  &>/dev/null &
    printf "Residential Address API started"
fi



exit
