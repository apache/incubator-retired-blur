rm ../blur-thrift/src/main/java/com/nearinfinity/blur/thrift/generated/*
rm -r gen-java/ gen-perl/ gen-rb/
thrift --gen perl --gen java --gen rb Blur.thrift
cp -r gen-java/* ../blur-thrift/src/main/java/
