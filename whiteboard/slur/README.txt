
== Overview ==
This project explores using SolrJ to work with a Blur installation.

== Mismatches ==
o) Commits - Blur commits on an update; Solr expects an explicit commit and rather 
	 than buffering, I've chosen to go ahead and follow Blur's model.
o) Optimize - Solr offers waitFlush, waitSearcher and Blur just offers maxSegments.
o) Deletes - The id's being passed to delete are understood to be RowIDs.  I gather that
   Solr can delete child docs directly using this method but I don't yet see a safe way
   to do that for us given only a single id. Maybe there's a way to do record deletions
   by establishing a convention (e.g. "1->5", would be recordid:5 row:1)?  
o) Row/SolrInputDocument/SolrDocument:
 - o) Indexing:
 		- - o) SolrInputDocument supports a Parent/Child document similar to Blur's Row/Record
 					 relationship, only in Solr the parent is a "real" document that can itself have fields.
 		- - o) The mismatch is currently addressed by throwing an IllegalArgumentException if a parent
 					 document is passed with fields. Given the need to round-trip the data, this seemed
 					 the only reasonable compromise.
 - o) Searching:
 		- - o) At the API level, Solr doesn't seem to support the notion of a Row (or parent document) 
 					 query, so searching is currently all Record queries.  Need to figure out if there's
 					 a way to expose Row query through SolrJ without parsing the query for the join syntax -
 					 which would again expose a mismatch because of the lack of child docs on returns. 					 
 					 
== Usage ==
	
	= Create a server = 
    SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), table);
    
  = Add a document =
    SolrInputDocument parent = new SolrInputDocument();
    parent.addField("rowid", "1");
    
    SolrInputDocument child = new SolrInputDocument();
    child.addField("recordid", "1");
    child.addField("fam.value", "123");
    
		parent.addChildDocument(child);
		
    server.add(parent);
    
  = Or add in batches =
    List<SolrInputDocument> docs = Lists.newArrayList();

    for (int i = 0; i < 100; i++) {
     //follow above example...
    }

    server.add(docs);
    
  = Delete row/rows = 
    server.delete("1");
    
    or
    
    List<String> ids = Lists.newArrayList("1", "2", "3", "4", "5");
    server.deleteById(ids);
    
  = Search (records) =
    SolrQuery query = new SolrQuery("value0-0");

    QueryResponse response = server.query(query);

    System.out.println("Total result count: " + response.getResults().getNumFound());

    SolrDocument docResult = response.getResults().get(0);

    assertEquals("0", docResult.getFieldValue("recordid"));
    assertEquals("value0-0", docResult.getFieldValue("fam.value"));

    Collection<Object> mvfVals = docResult.getFieldValues("fam.mvf");

    assertTrue("We should get all our values back[" + mvfVals + "]",
        CollectionUtils.isEqualCollection(mvfVals, Lists.newArrayList("value0-0", "value0-0")));
    
    == Notes ==
    **caveat is that I don't have experience with SolrJ, so this may very
    well be dangerous.
