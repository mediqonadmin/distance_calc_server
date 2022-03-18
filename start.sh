
#!/bin/sh

sudo apt update
sudo apt-get install p7zip

rm -rf osrm

mkdir osrm
cd osrm


wget https://github.com/mediqonadmin/distance_calc_server/raw/main/distance_calc_server.7z

tar xvzf distance_calc_server.tar.gz

rm distance_calc_server.tar.gz

./prepare_data.sh
