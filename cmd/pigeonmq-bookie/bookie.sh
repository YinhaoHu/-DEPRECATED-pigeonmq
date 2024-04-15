#!/bin/bash

# bookie.sh is a convenient tool to start/close a bookie process.

if [ $# != 2 ];then
  echo "Usage: $0 <bookie_name> <option: start/close>"
  exit 1
fi

VALID_BOOKIE_NAMES=("bookie_1" "bookie_2" "bookie_3")
BOOKIE_NAME=$1

# Check if BOOKIE_NAME is in the VALID_BOOKIE_NAMES array
if [[ ! " ${VALID_BOOKIE_NAMES[@]} " =~ " ${BOOKIE_NAME} " ]]; then
  echo "Invalid bookie name. Valid names are: ${VALID_BOOKIE_NAMES[*]}"
  exit 1
fi

PID_FILE=/var/run/$BOOKIE_NAME.pid
LOCK_FILE=/var/lock/$BOOKIE_NAME.lock

BIN=/home/hoo/Filebase/project/pigeonmq/cmd/pigeonmq-bookie/pigeonmq-bookie
CONFIG=/home/hoo/Filebase/project/pigeonmq/configs/$BOOKIE_NAME.cfg

# Handle different option.
OPTION=$2
if [[ $OPTION == "start" ]]; then
  sudo daemonize -p $PID_FILE -l $LOCK_FILE $BIN --config $CONFIG
  if [[ $? == 0 ]]; then
    echo "Bookie $BOOKIE_NAME started..."
    echo "process id :$(cat $PID_FILE) "
  else
    echo "Error occurred in starting bookie $BOOKIE_NAME."
  fi

elif [[ $OPTION == "close" ]]; then
  sudo kill -s SIGTERM $(cat $PID_FILE)
  if [[ $? == 0 ]]; then
    echo "Bookie $BOOKIE_NAME terminated successfully."
  else
    echo "Error occurred in terminating bookie $BOOKIE_NAME."
  fi
else
  echo "Invalid option: $2"
fi



