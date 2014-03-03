package org.apache.blur.slur;

import java.util.List;

import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.utils.BlurConstants;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class BlurResultHelper {

  public static SolrDocumentList from(BlurResults results) {
    SolrDocumentList docResults = new SolrDocumentList();
    
    convertMetadata(results, docResults);

    convertRows(results.getResults(), docResults);
    
    return docResults;
  }

  private static void convertRows(List<BlurResult> results, SolrDocumentList docResults) {
    for(BlurResult result: results) {      
      docResults.add(convertRecord(result.getFetchResult().getRecordResult()));
    }
    
  }

  private static SolrDocument convertRecord(FetchRecordResult recResult) {
    SolrDocument doc = new SolrDocument();
    Record record = recResult.getRecord();
    
    doc.addField(BlurConstants.RECORD_ID, record.getRecordId());
    
    for(Column col: record.getColumns()) {
      System.out.println("Adding[" + joinColumnFamily(record.getFamily(), col.getName()) + "] val[" + col.getValue() + "]");
      doc.addField(joinColumnFamily(record.getFamily(), col.getName()), col.getValue());
    }
    
    return doc;
  }

  private static String joinColumnFamily(String family, String name) {
    if(family != null) {
      return family + "." + name;
    }
    return name;
  }

  private static void convertMetadata(BlurResults results, SolrDocumentList docResults) {
    docResults.setNumFound(results.getTotalResults());
    docResults.setStart(results.getQuery().getStart());
  }
}
