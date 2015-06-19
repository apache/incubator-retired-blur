== Overview ==
This provides some info on testing the CSD/Parcel packaging for Apache Blur.

== CSD ==
 sudo cp distribution/target/BLUR-0.2.4.jar /opt/cloudera/csd/BLUR-0.2.4.jar
 sudo service cloudera-scm-server restart
 
== Parcel ==
Note that the '.tar.gz' extension is ripped off in the copy
sudo cp distribution/target/blur-hadoop2-2.5.0-cdh5.2.0-0.2.4-incubating-SNAPSHOT-el6.parcel.tar.gz /opt/cloudera/parcel-repo/blur-hadoop2-2.5.0-cdh5.2.0-0.2.4-incubating-SNAPSHOT-el6.parcel

Then, in /opt/cloudera/parcel-repo:
sha1sum blur-hadoop2-2.5.0-cdh5.2.0-0.2.4-incubating-SNAPSHOT-el6.parcel | awk '{print $1}' > blur-hadoop2-2.5.0-cdh5.2.0-0.2.4-incubating-SNAPSHOT-el6.parcel.sha && cat blur-hadoop2-2.5.0-cdh5.2.0-0.2.4-incubating-SNAPSHOT-el6.parcel.sha

... which will write the sha file and print it to the screen.  You can then copy that into a manifest in the
 same directory [/opt/cloudera/parcel-repo/manifest.json] and substitute it in the hash value, which will look
 like this:
 
 
{
  "lastUpdated":14286139390000,
  "parcels": [
    {
      "parcelName":"blur-hadoop2-2.5.0-cdh5.2.0-0.2.4-incubating-SNAPSHOT-el6.parcel",
      "components": [
        {
          "name" : "blur",
          "version" : "0.2.4",
          "pkg_version": "0.2.4"
        }
      ],
      "hash":"632bd8d320f27ba68b9595f39ca2d99a203dd43c"
    }
  ]
}
 
Now you should be able to click the "Check for new parcels" in CM and begin activating it.