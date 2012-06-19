#!/bin/zsh
loopno=1
echo "loop starting..."
echo "-----------------------------"
for file in $PWD/*
do	
	if [ -d $file ]; then
		continue
	fi
	temp="no"
	cat $file|grep opsw | read temp
#    echo $temp
    if [ "$temp" != "no" ] && [ "$temp" != "" ] && [[ $file != *find_opsw.sh ]]
    then
    	echo $loopno
	loopno=$((loopno + 1))
	echo $file 
	echo $temp
    fi
done
echo "-----------------------------"
echo "done."
