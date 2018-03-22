#!/bin/bash
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

cd $SCRIPTPATH/.. && mvn -Dcheckstyle.skip=true -Dmaven.test.skip=true -Dmaven.javadoc.skip=true package && cd $SCRIPTPATH

ROARINGPATH=$SCRIPTPATH"/../roaringbitmap/target/*"
for filename in *.java; do
  nonext="${filename%.*}"
  echo $nonext
  javac -cp "$ROARINGPATH" $filename && java -cp $ROARINGPATH:. $nonext
  echo
done
