== Overview ==
This project explores using SolrJ to work with a Blur installation.

== Mismatches ==
o) Commits - Blur commits on an update; Solr expects and explicit - rather 
	 than buffering, I've chosen to go ahead and follow Blur's model.
o) Row/SolrInputDocument - The parent document in Solr seems to be a "real" 
	 document, with fields. So far, you can either have subdocuments or fields
	 on the main document, but not both.
o) Optimize - Solr offers waitFlush, waitSearcher and Blur just offers maxSegments.
	 
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
    
    
    == Notes ==
    **caveate is that I don't have experience with SolrJ, so this may very
    well be dangerous.
