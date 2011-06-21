rm -r gen-java/*
rm gen-rb/*

thrift --gen java --gen rb Blur.thrift
