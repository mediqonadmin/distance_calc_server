
#!/bin/sh

echo "================================================================================================================================"
echo "Install git if is not installed"

ROOT_DIR="/home/${USER}/osrm"

if [ -d "$ROOT_DIR" ]; then
  echo "Folder ${ROOT_DIR} does exists"
else
  echo "Folder ${ROOT_DIR} does not exists"
  Exit code 1
fi

cd osrm


echo "================================================================================================================================"
echo "Downloading geo files ..."

mkdir pre_data

cd pre_data

wget http://download.geofabrik.de/europe/france/alsace-latest.osm.pbf

wget http://download.geofabrik.de/europe/austria-latest.osm.pbf

wget http://download.geofabrik.de/europe/belgium-latest.osm.pbf

wget http://download.geofabrik.de/europe/czech-republic-latest.osm.pbf

wget http://download.geofabrik.de/europe/denmark-latest.osm.pbf

wget http://download.geofabrik.de/europe/poland/dolnoslaskie-latest.osm.pbf

wget http://download.geofabrik.de/europe/netherlands/drenthe-latest.osm.pbf

wget http://download.geofabrik.de/europe/netherlands/friesland-latest.osm.pbf

wget http://download.geofabrik.de/europe/netherlands/gelderland-latest.osm.pbf

wget http://download.geofabrik.de/europe/germany-latest.osm.pbf

wget http://download.geofabrik.de/europe/netherlands/groningen-latest.osm.pbf

wget http://download.geofabrik.de/europe/netherlands/limburg-latest.osm.pbf

wget http://download.geofabrik.de/europe/france/lorraine-latest.osm.pbf

wget http://download.geofabrik.de/europe/poland/lubuskie-latest.osm.pbf

wget http://download.geofabrik.de/europe/luxembourg-latest.osm.pbf

wget http://download.geofabrik.de/europe/netherlands/overijssel-latest.osm.pbf

wget http://download.geofabrik.de/europe/switzerland-latest.osm.pbf

wget http://download.geofabrik.de/europe/poland/zachodniopomorskie-latest.osm.pbf


echo "================================================================================================================================"
echo "Downloading geo files is done."
echo "================================================================================================================================"
echo "Converting geo files .."

cd ..
mkdir converted_data

./osmconvert64  ./pre_data/alsace-latest.osm.pbf -o=./converted_data/alsace.o5m
./osmconvert64  ./pre_data/austria-latest.osm.pbf -o=./converted_data/austria.o5m
./osmconvert64  ./pre_data/belgium-latest.osm.pbf -o=./converted_data/belgium.o5m
./osmconvert64  ./pre_data/czech-republic-latest.osm.pbf -o=./converted_data/czech_republic.o5m
./osmconvert64  ./pre_data/denmark-latest.osm.pbf -o=./converted_data/denmark.o5m
./osmconvert64  ./pre_data/dolnoslaskie-latest.osm.pbf -o=./converted_data/dolnoslaskie.o5m
./osmconvert64  ./pre_data/drenthe-latest.osm.pbf -o=./converted_data/drenthe.o5m
./osmconvert64  ./pre_data/friesland-latest.osm.pbf -o=./converted_data/friesland.o5m
./osmconvert64  ./pre_data/gelderland-latest.osm.pbf -o=./converted_data/gelderland.o5m
./osmconvert64  ./pre_data/germany-latest.osm.pbf -o=./converted_data/germany.o5m
./osmconvert64  ./pre_data/groningen-latest.osm.pbf -o=./converted_data/groningen.o5m
./osmconvert64  ./pre_data/limburg-latest.osm.pbf -o=./converted_data/limburg.o5m
./osmconvert64  ./pre_data/lorraine-latest.osm.pbf -o=./converted_data/lorraine.o5m
./osmconvert64  ./pre_data/lubuskie-latest.osm.pbf -o=./converted_data/lubuskie.o5m
./osmconvert64  ./pre_data/luxembourg-latest.osm.pbf -o=./converted_data/luxembourg.o5m
./osmconvert64  ./pre_data/overijssel-latest.osm.pbf -o=./converted_data/overijssel.o5m
./osmconvert64  ./pre_data/switzerland-latest.osm.pbf -o=./converted_data/switerland.o5m
./osmconvert64  ./pre_data/zachodniopomorskie-latest.osm.pbf -o=./converted_data/zachodniopomorskie.o5m

echo "================================================================================================================================"
echo "Converting geo files is done."

echo "================================================================================================================================"
echo "Merging geo files in germany_eu.osm.pbf ..."


mkdir all_data

./osmconvert64 ./converted_data/alsace.o5m ./converted_data/austria.o5m ./converted_data/belgium.o5m ./converted_data/czech_republic.o5m ./converted_data/denmark.o5m ./converted_data/dolnoslaskie.o5m ./converted_data/drenthe.o5m ./converted_data/friesland.o5m ./converted_data/gelderland.o5m ./converted_data/germany.o5m ./converted_data/groningen.o5m ./converted_data/limburg.o5m ./converted_data/lorraine.o5m ./converted_data/lubuskie.o5m ./converted_data/luxembourg.o5m ./converted_data/overijssel.o5m ./converted_data/switerland.o5m ./converted_data/zachodniopomorskie.o5m -o=./all_data/germany_eu.osm.pbf

echo "================================================================================================================================"
echo "Merging geo files in germany_eu.osm.pbf is done."


echo "================================================================================================================================"
echo "Delete geo part files ..."

rm -rf pre_data
rm -rf converted_data

echo "================================================================================================================================"
echo "Delete geo part files done."

echo "================================================================================================================================"
echo "Preparing osrm files ..."

./osrm-extract ./all_data/germany_eu.osm.pbf -p ./profiles/car.lua

./osrm-contract ./all_data/germany_eu.osm.pbf

echo "================================================================================================================================"
echo "Preparing osrm files done."

