# Regression Testing
## A documented set of steps needed to manually regression test a page:

* Dashboard
  1. Click on a zookeeper, this should take you to that zookeepers environment page and set it as the current zookeeper.
  2. Click on an hdfs, this should take you to the hdfs page and open up the root of the selected hdfs instance.
  3. Change the state of a few of the shards and controllers, make sure that the zookeeper widgets update properly. This includes the icon states and the number of online and offline shards/controllers.
  4. Change the state of the hdfs, make sure that the hdfs widgets update properly.
  5. Start a new session, and click on the Environment header link, this should prompt a popup asking for a Zookeeper to set as the current.

  Last Completed: 1/7/2013

* Environment Page
  1. Assure that the values in the tables track the state of the nodes (similar to the Dashboard page). Change values and assure that the values in the tables update.
  2. Change the zookeeper status and assure that the header changes color and text properly.
  3. Set the zookeeper, controller's, and shard's status to offline. While offline they should have an 'x' icon that will 'forget' the nodes. Assure that this is true and that by clicking each one the item is removed.

  Last Completed: 1/7/2013

* Blur Tables Page


* Blur Queries Page
  1. Run, or create, a few queries and assure that they appear in the queries table.
  2. Set the "Queries in the last" option to the different values and assure that they are being hidden / shown properly.
  3. Set the "Quick filter" to the different values and assure that the queries are being properly filtered.
  4. Set the "Auto Refresh" to the different values and assure that the server is being queried at the new interval.
  5. Test a few different filters in the "Filter Queries" box, be sure to test the hidden column state.
  6. Test that the columns sort properly when sorting by each header.

  Last Completed: 1/7/2013