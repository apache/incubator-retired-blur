
== Overview ==
This project explores using SolrJ to work with a Blur installation.

== Mismatches ==
o) Commits - Blur commits on an update; Solr expects an explicit commit and rather 
	 than buffering, I've chosen to go ahead and follow Blur's model.
o) Row/SolrInputDocument - The parent document in Solr seems to be a "real" 
	 document, with fields. So far, you can either have subdocuments or fields
	 on the main document, but not both.
o) Optimize - Solr offers waitFlush, waitSearcher and Blur just offers maxSegments.
o) Deletes - The id's being passed to delete are understood to be RowIDs.  I gather that
   Solr can delete child docs directly using this method but I don't yet see a safe way
   to do that for us given only a single id. Maybe there's a way to do record deletions
   by establishing a convention (e.g. "1->5", would be recordid:5 row:1)?  
	 
== Usage ==
	
	= Create a server = 
    SolrServer server = new SolrLookingBlurServer(miniCluster.getControllerConnectionStr(), table);
    
  = Add a document =
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    doc.addField("fam.value", "123");

    server.add(doc);
    
  = Or add in batches =
    List<SolrInputDocument> docs = Lists.newArrayList();

    for (int i = 0; i < 100; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("fam.value", "12" + i);
      docs.add(doc);
    }

    server.add(docs);
    
  = Delete row/rows = 
    server.delete("1");
    
    or
    
    List<String> ids = Lists.newArrayList("1", "2", "3", "4", "5");
    server.deleteById(ids);
    
    == Notes ==
    **caveat is that I don't have experience with SolrJ, so this may very
    well be dangerous.
