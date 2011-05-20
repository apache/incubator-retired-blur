package com.nearinfinity.agent;

public class Agent {

	public static void main(String[] args) {

	}

}


/*
public class SampleHdfsCode {

    public static void main(String[] args) throws URISyntaxException, IOException {
//        URI uri = new URI("hdfs://namenode:9000/");
        URI uri = new URI("file:///");//local file system for testing without hdfs running
        FileSystem fileSystem = FileSystem.get(uri,new Configuration());
        FileStatus[] listStatus = fileSystem.listStatus(new Path("file:///Users"));
        for (FileStatus fileStatus : listStatus) {
            System.out.println(fileStatus.getPath().toString());
        }
    }
}
*/