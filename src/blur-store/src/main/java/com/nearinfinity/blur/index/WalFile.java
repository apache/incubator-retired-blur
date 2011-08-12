package com.nearinfinity.blur.index;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class WalFile {
    
    public enum TYPE {
        ADD(0),
        DELETE(1),
        UPDATE(2);
        
        private int _code;

        private TYPE(int code) {
            _code = code;
        }
        
        public int code() {
            return _code;
        }
        
        public static TYPE lookup(int code) {
            switch (code) {
            case 0:
                return ADD;
            case 1:
                return DELETE;
            case 2:
                return UPDATE;
            default:
                throw new RuntimeException("Code not found");
            }
        }
    }
    
    private static final Log LOG = LogFactory.getLog(WalFile.class);
    
    public static List<Document> readBlockOfDocs(IndexInput input) throws IOException {
        int size = input.readVInt();
        List<Document> docs = new ArrayList<Document>(size);
        for (int i = 0; i < size; i++) {
            readDoc(input,docs);
        }
        return docs;
    }
    
    public static void readDoc(IndexInput input, List<Document> docs) throws IOException {
        int numberOfFields = input.readVInt();
        Document document = new Document();
        for (int i = 0; i < numberOfFields; i++) {
            readField(input,document);
        }
        docs.add(document);
    }
    
    public static void readField(IndexInput input, Document document) throws IOException {
        String name = input.readString();
        int type = input.readVInt();
        if (type == 0) {
            int length = input.readVInt();
            byte[] buffer = new byte[length];
            input.readBytes(buffer, 0, length);
            document.add(new Field(name,buffer));
        } else if (type == 1) {
            String value = input.readString();
            document.add(new Field(name,value,Store.YES,Index.ANALYZED_NO_NORMS));
        } else if (type == 2) {
            String value = input.readString();
            document.add(new Field(name,value,Store.NO,Index.ANALYZED_NO_NORMS));
        } else {
            throw new RuntimeException("unknown type [" + type + "]");
        }
    }

    public static void writeDocument(IndexOutput output, Document document) throws IOException {
        List<Fieldable> fields = document.getFields();
        output.writeVInt(fields.size());
        for (Fieldable fieldable : fields) {
            writeField(output, fieldable);
        }
    }
    
    public static void writeField(DataOutput output, Fieldable fieldable) throws IOException {
        output.writeString(fieldable.name());
        if (fieldable.isBinary()) {
            output.writeVInt(0);
            int len = fieldable.getBinaryLength();
            int off = fieldable.getBinaryOffset();
            byte[] bs = fieldable.getBinaryValue();
            output.writeVInt(len);
            output.writeBytes(bs, off, len);
        } else if (!fieldable.isBinary() && fieldable.isStored()) {
            output.writeVInt(1);
            output.writeString(fieldable.stringValue());
        } else if (!fieldable.isBinary() && !fieldable.isStored()) {
            output.writeVInt(2);
            output.writeString(fieldable.stringValue());
        } else {
            throw new RuntimeException("unknown field type [" + fieldable + "]");
        }
    }
    
    public static void writeActionDocumentAdd(IndexOutput output, Document doc) throws IOException {
        output.writeVInt(TYPE.ADD.code());//add code
        output.writeVInt(1);//add number of documents
        WalFile.writeDocument(output,doc);
    }

    public static void writeActionDocumentsAdd(IndexOutput output, Collection<Document> docs) throws IOException {
        output.writeVInt(TYPE.ADD.code());//add code
        int size = docs.size();
        output.writeVInt(size);
        for (Document document : docs) {
            WalFile.writeDocument(output,document);
        }
    }

    public static void writeActionDocumentsUpdate(IndexOutput output, Term term, Collection<Document> docs) throws IOException {
        output.writeVInt(TYPE.UPDATE.code());//add code
        writeTerm(output,term);
        int size = docs.size();
        output.writeVInt(size);
        for (Document document : docs) {
            WalFile.writeDocument(output,document);
        }
    }

    public static void writeActionDocumentUpdate(IndexOutput output, Term term, Document doc) throws IOException {
        output.writeVInt(TYPE.UPDATE.code());//add code
        writeTerm(output,term);
        output.writeVInt(1);//add number of documents
        WalFile.writeDocument(output,doc);
    }

    public static void writeActionDocumentsDelete(IndexOutput output, Query[] queries) {
        throw new RuntimeException("not implemented");
    }

    public static void writeActionDocumentsDelete(IndexOutput output, Term[] terms) throws IOException {
        for (int i = 0; i < terms.length; i++) {
            output.writeVInt(TYPE.DELETE.code());//add code
            writeTerm(output,terms[i]);
        }
    }
    
    private static void writeTerm(IndexOutput output, Term term) throws IOException {
        output.writeString(term.field());
        output.writeString(term.text());
    }

    public static long replayInput(IndexInput input, WalIndexWriter walIndexWriter) throws CorruptIndexException, IOException {
        long total = 0;
        List<Document> docs = null;
        Term term = null;
        try {
            while (true) {
                TYPE type = WalFile.readType(input);
                switch (type) {
                case ADD:
                    docs = WalFile.readBlockOfDocs(input);
                    if (docs == null) {
                        return total;
                    }
                    total += docs.size();
                    walIndexWriter.addDocuments(false,docs);
                    break;
                case UPDATE:
                    term = readTerm(input);
                    docs = WalFile.readBlockOfDocs(input);
                    if (docs == null) {
                        return total;
                    }
                    total += docs.size();
                    walIndexWriter.updateDocuments(false,term,docs);
                    break;
                case DELETE:
                    term = readTerm(input);
                    walIndexWriter.deleteDocuments(false, term);
                    break;
                default:
                    break;
                }
            }
        } catch (EOFException e) {
            return total;
        } catch (IOException e) {
            if (e.getMessage().equals("read past EOF")) {
                return total;
            }
            throw e;
        }
    }

    private static Term readTerm(IndexInput input) throws IOException {
        return new Term(input.readString(),input.readString());
    }

    private static TYPE readType(IndexInput input) throws IOException {
        int code = input.readVInt();
        return TYPE.lookup(code);
    }
}
