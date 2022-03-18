
#!/bin/sh

sudo apt update
sudo apt-get install p7zip

sudo sudo apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev curl libbz2-dev

rm -rf osrm

mkdir osrm
cd osrm


wget https://github.com/mediqonadmin/distance_calc_server/raw/main/distance_calc_server.7z

7zr x distance_calc_server.7z

rm distance_calc_server.7z

sudo chmod 777 *.sh

sudo chmod 777 ./binding/*


