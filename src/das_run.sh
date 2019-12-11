#!/bin/sh

# This script can be used for running the map reduce framework on the das
# > module load python/3.6.0
# > module load prun
# > prun -v -1 -np 2 ./das_run.sh {program arguments}

# SGE destroys path, but saves it in SGE_O_PATH. Reset it.
if [ -z "$SGE_O_PATH" ]
then	:
else    export PATH="$SGE_O_PATH"
fi

# Filter rank and nhosts if present.
NHOSTS=`echo $PRUN_HOSTNAMES | awk '{print NF}'`
case "X${1}X$2" in
X${PRUN_CPU_RANK}X$NHOSTS)
    shift
    shift
    ;;
esac

# Get the first host.
for i in $HOSTS
do
    serverhost=$i
    break
done

# Start a server instance
echo "python3 main.py -i $PRUN_CPU_RANK -w $NHOSTS -nh $serverhost $@"
python3 main.py -i $PRUN_CPU_RANK -w $NHOSTS -nh $serverhost $@
