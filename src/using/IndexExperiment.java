package using;

import java.io.IOException;
import java.util.UUID;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class IndexExperiment {

	public static void main(String[] args) throws Exception {
		Directory directory = genIndex();
		Directory directory2 = genIndex();
		
		IndexReader indexReader = IndexReader.open(directory);
		System.out.println(indexReader.numDocs());
		
		IndexWriter indexWriter = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		indexWriter.addIndexes(IndexReader.open(directory2));
		indexWriter.close();
		
		indexReader = indexReader.reopen();

		System.out.println(indexReader.numDocs());
		
		indexWriter = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		indexWriter.addIndexes(IndexReader.open(directory2));
		indexWriter.rollback();
		indexWriter.close();
		
		indexReader = indexReader.reopen();

		System.out.println(indexReader.numDocs());
	}

	private static Directory genIndex() throws CorruptIndexException, IOException {
		RAMDirectory directory = new RAMDirectory();
		IndexWriter indexWriter = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		
		indexWriter.addDocument(getDoc());
		indexWriter.addDocument(getDoc());
		indexWriter.addDocument(getDoc());
		
		indexWriter.close();
		
		return directory;
	}

	private static Document getDoc() {
		Document document = new Document();
		document.add(new Field("name","value",Store.YES,Index.ANALYZED_NO_NORMS));
		document.add(new Field("id",UUID.randomUUID().toString(),Store.YES,Index.ANALYZED_NO_NORMS));
		return document;
	}

}
