rm ../src/blur-thrift/src/main/java/com/nearinfinity/blur/thrift/generated/*
rm -r gen-java/ gen-perl/ gen-rb/ gen-html/
thrift --gen html --gen perl --gen java --gen rb Blur.thrift
cp -r gen-java/* ../src/blur-thrift/src/main/java/
