#!/usr/bin/env bash

tsnum=$1
echo "tsnum: [$tsnum]"
queries=$2
thresh=${3:=((6/10))}
gridSize=${5:=(2)}
tsdir='/tmp/ts_256_$tsnum'
workdir=${6:='/tmp'}

#Path variables
HADOOP=${HADOOP:='/tmp/hadoop'}
SPARK=${SPARK:='/tmp/spark'}

nodes=$(wc -l < nodes)
cores=$(cat /proc/cpuinfo | grep processor | wc -l)

### Data generation ###

#if (( $# < 6 )); then
if [[ $4 =~ ^-?[0-9]+$ ]]; then 
 numDir=${4:=(5)} 
 echo "numDir: [$numDir]"
 numfiles=$(($tsnum/10000/$numDir))
 
 if [ ! -e $tsdir/listFiles.txt ]; then 
   #rm -rf $tsdir
   mkdir -p $tsdir
   for i in $(seq 1 $numDir)
   do
     echo ts_256_$tsnum$i >> $tsdir/listFiles.txt
   done
 fi

 timestp0=$(date +%s)
 echo "Data Generation"

 for file in `cat $tsdir/listFiles.txt`
  do
     if ! $( $HADOOP/bin/hadoop fs -test -d $file ); then
        $SPARK/bin/spark-submit --class fr.inria.zenith.DataGenerator $workdir/tsGen-scala-1.0-SNAPSHOT.jar --numFiles $numfiles --tsNum $(($tsnum/$numDir)) --tsSize 256 --outputPath $file
     fi
  done

  timestp1=$(date +%s)
  echo "Data generation (elapsed time): $((($timestp1 - $timestp0)/60)) minutes ..."

else ## Given Input file ##

#if ! [[ $4 =~ ^-?[0-9]+$ ]]; then
 #if (( $# > 5 )); then
 numDir=1
 echo "Input file: $4"
 rm -rf $tsdir
 mkdir $tsdir
 echo $4 >> $tsdir/listFiles.txt
fi


numpart=$(($tsnum/10000/$numDir))

### Grid construction ###

for file in `cat $tsdir/listFiles.txt`
  do
    echo "file: [$file]"
	$SPARK/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti $workdir/parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath $file --sizeSketches 30 --gridDimension 2 --gridSize $gridSize --batchSize 10000 --gridConstruction true  --numPart $numpart --tsNum $tsnum
  done

timestp2=$(date +%s)
echo "Grid construction (total elapsed time): $((($timestp2 - $timestp1)/60)) minutes ..."

### Index creation ###
$SPARK/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti $workdir/parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --sizeSketches 30 --gridDimension 2 --gridSize $gridSize --numPart $numpart --tsNum $tsnum

timestp3=$(date +%s)

### Query processing ###

if ! $( $HADOOP/bin/hadoop fs -test -d queries_$queries ); then
 $SPARK/bin/spark-submit --class fr.inria.zenith.DataGenerator $workdir/tsGen-scala-1.0-SNAPSHOT.jar --numFiles 1 --tsNum $queries --tsSize 256 --outputPath queries_$queries
fi

$SPARK/bin/spark-submit --class fr.inria.zenith.adt.TSToDBMulti $workdir/parSketch-1.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath ts_256_$tsnum --sizeSketches 30 --gridDimension 2 --gridSize $gridSize --queryFilePath queries_$queries --candThresh $thresh --numPart $numpart --tsNum $tsnum

timestp4=$(date +%s)