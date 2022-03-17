#!/bin/sh

echo "================================================================================================================================"
echo "Starting osrm server ..."

OSRM_ROOT_DIR="/home/${USER}/osrm/binding"

if [ -d "$OSRM_ROOT_DIR" ]; then
  
  "Folder ${OSRM_ROOT_DIR} exists"
  
else
  echo "Folder ${OSRM_ROOT_DIR} does not exists"
  Exit code 1
fi

OSRM_ROUTED_PATH="${OSRM_ROOT_DIR}/osrm-routed"

if [ -d "$OSRM_ROUTED_PATH" ]; then
  
  "File ${OSRM_ROUTED_PATH} exists"
  
else
  echo "File ${OSRM_ROUTED_PATH} does not exists"
  Exit code 1
fi

DATA_PATH="${OSRM_ROOT_DIR}/all_data/germany_eu.osm.pbf"

if [ -d "$DATA_PATH" ]; then
  
  "File ${DATA_PATH} exists"
  
else
  echo "File ${DATA_PATH} does not exists"
  Exit code 1
fi


./"$OSRM_ROUTED_PATH" "$DATA_PATH" -p 5000

