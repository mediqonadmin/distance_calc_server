
#!/bin/sh

rm -rf osrm

mkdir osrm
cd osrm

wget https://github.com/mediqonadmin/distance_calc_server/raw/main/distance_calc_server.tar.gz

tar xvzf distance_calc_server.tar.gz

rm distance_calc_server.tar.gz

