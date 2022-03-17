#!/bin/sh

echo "================================================================================================================================"
echo "Prepairing python ..."

sudo apt update
sudo sudo apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev curl libbz2-dev


WORKING_DIR="/home/${USER}/python"

if [ -d "$WORKING_DIR" ]; then
  
  rm -rf "$WORKING_DIR"
  
fi
mkdir "$WORKING_DIR"
cd "$WORKING_DIR"

echo "================================================================================================================================"
echo "Downloading python ..."

wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tar.xz

echo "================================================================================================================================"
echo "Extracting python ..."

tar -xf Python-3.7.9.tar.xz

echo "================================================================================================================================"
echo "Installing python ..."

cd Python-3.7.9
./configure --enable-optimizations

make -j 8

sudo make altinstall

python3.7 --version





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

