#!/bin/bash

# Makes a drift-scan style movie

# List of file names
filelist=`cat $1`
echo "filelist = "$filelist
outfile=drift_movie.avi
# box size for making movie
movsize=2000

if [ -f $outfile ]; then
    rm $outfile
fi

if [ -d jpegs ]; then
	rm -rf jpegs
    #rm jpegs/*.jpg
    #rm jpegs/*.fits
#else
fi
mkdir jpegs


# Set the scale to be consistent throughout
firstfile=`head -1 $1`
min_max=$(python pct.py $firstfile)

echo "First file = "$firstfile
echo "min_max ="$min_max

for file in $filelist
do
  echo $file
  obsid=`echo $file | awk 'BEGIN {FS="."} {print $1}'`
  echo "Processing $file"
  outname="./jpegs/${obsid}.jpg"
  x=`/mnt/gleam/software/bin/fitshdr $file | grep CRPIX1 | awk '{printf("%d\n",$3)}'`
  y=`/mnt/gleam/software/bin/fitshdr $file | grep CRPIX2 | awk '{printf("%d\n",$3)}'`
  if [ ! -f "$outname" ] ; then
  	echo "ds9 cropping"
  	  /mnt/gleam/software/bin/ds9 -grid  $file -crop $x $y $movsize $movsize -cmap Heat -scale limits $min_max -zoom 0.25 -saveimage "$outname" -exit
     #/mnt/gleam/software/bin/ds9 -grid yes $file -crop $x $y $movsize $movsize -cmap Heat -scale limits $min_max  -export "$outname" -exit
     # /mnt/gleam/software/bin/ds9 -grid $file  -crop $x $y $movsize $movsize -cmap Heat -scale limits $min_max -saveimage "$outname" -exit
  fi
done

mencoder "mf://./jpegs/*.jpg" -mf fps=3 -o $outfile -ovc lavc -lavcopts vcodec=msmpeg4v2:vbitrate=1100
