#!/bin/sh

cat ../NOTICE-bin.txt

for f in `ls *.NOTICE` 
do
 if [ -s $f ]; then 
   echo ================== NOTICE for: $f | sed -e 's/\.NOTICE//'
   cat $f
   echo 
 fi
done
