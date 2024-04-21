#!/bin/bash

# config.sh is a helper shell script to make configuration management easily in development.
# Including make these configurations synchronized.

function SyncConfigs() {
  cp bookie_1.cfg bookie_2.cfg
  sed -i 's/storage1/storage2/g' bookie_2.cfg
  sed -i 's/19001/19002/g' bookie_2.cfg

  cp bookie_1.cfg bookie_3.cfg
  sed -i 's/storage1/storage3/g' bookie_3.cfg
  sed -i 's/19001/19003/g' bookie_3.cfg

  echo "Configurations are synchronized."
}

if [[ $# != 1 ]]; then
  echo "Usage: $0 <option:sync>"
  exit 1
fi

OPTION=$1
if [[ $OPTION == "sync" ]]; then
  SyncConfigs
  exit 0
else
  echo "Invalid option: $OPTION"
  exit 1
fi