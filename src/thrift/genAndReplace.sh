rm ../blur-thrift/src/main/java/com/nearinfinity/blur/thrift/generated/*
rm -r gen-java/
thrift --gen java Blur.thrift
cp -r gen-java/* ../blur-thrift/src/main/java/
