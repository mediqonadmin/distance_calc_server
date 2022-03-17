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

sudo make alt install

python3.7 --version


echo "================================================================================================================================"
echo "Installing python done."


