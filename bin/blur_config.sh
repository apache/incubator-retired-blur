#!/bin/sh
export BLUR_LOG=$BLUR_HOME/logs
export BLUR_LIB=$BLUR_HOME/libs
export BLUR_PID_DIR=/tmp/blur

mkdir -p $BLUR_PID_DIR

c=1
for i in `ls $BLUR_LIB/*.jar`
do
  if [ "$c" -eq "1" ]; then
    BLUR_CLASS_PATH=${i}
    c=2
  else
    BLUR_CLASS_PATH=${BLUR_CLASS_PATH}:${i}
  fi
done
export BLUR_CLASS_PATH

if [ ! -d $BLUR_LOG ]; then
  mkdir $BLUR_LOG
fi




