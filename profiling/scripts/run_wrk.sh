#!/bin/bash

NUMS=(2 4 4 12 8 150)
SCRIPTS=$(ls -r *.lua)
DURATION=2m
URL_PATH=http://localhost:8080

for ((i = 0; i < ${#NUMS[*]}; i += 2 ))
do

     echo -e "\nStart service..."
     (cd ../../
     ./gradlew -q run
     ) & sleep 15


    THREADS=${NUMS[i]}
    CONNECTIONS=${NUMS[i+1]}

    for SCRIPT in ${SCRIPTS}
    do
        COMMAND="wrk --latency -t${THREADS} -c${CONNECTIONS} -d${DURATION} -s ${SCRIPT} ${URL_PATH}"
        echo -e "\n+${COMMAND}"
        eval ${COMMAND}
    done

    echo -e "\nStop service..."
    (cd ../../
     ./gradlew --stop
     ) & sleep 15


done