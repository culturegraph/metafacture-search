package org.culturegraph.mf.search.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 * @author Markus Michael Geipel
 *
 */
public final class BatchIndexer{

	public static final String DEFAULT_FIELD = "all";
	private static final Logger LOG = LoggerFactory.getLogger(BatchIndexer.class);
	private static final int DEFAUT_BATCH_SIZE =  10000;
	private static final String ID_NAME = "_id";
	private final IndexWriter indexWriter;

	private int batchSize = DEFAUT_BATCH_SIZE;
	private final List<Document> docBuffer = new ArrayList<Document>();
	private Document currentDoc;
	private int count;
	

	public BatchIndexer(final IndexWriter indexWriter) {
			this.indexWriter = indexWriter;
	}
	
	public int getCount() {
		return count;
	}
	
	public void startDocument(final String identifier) {
		currentDoc = new Document();
		add(new Field(ID_NAME, identifier, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
	}
	
	public void startDocument() {
		currentDoc = new Document();
	}

	public void endDocument() {
		if(!currentDoc.getFields().isEmpty()){
			docBuffer.add(currentDoc);
			++count;
		}
		
		if(count%batchSize==0){
			flush();			
		}
	}

	public void add(final Field field) {
		//LOG.info("adding field " + field);
		currentDoc.add(field);
	}
	
	public void flush() {
		try {
			indexWriter.addDocuments(docBuffer);
			docBuffer.clear();
			LOG.info(count + " records indexed");
		} catch (CorruptIndexException e) {
			throw new IndexException(e);
		} catch (IOException e) {
			throw new IndexException(e);
		}
	}
	

	public void setBatchSize(final int batchSize) {
		this.batchSize = batchSize;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void close()  {
		flush();

		try {
			LOG.info("commiting index");
			indexWriter.commit();

		} catch (CorruptIndexException e) {
			throw new IndexException(e);
		} catch (IOException e) {
			throw new IndexException(e);
		}
	}

	public IndexWriter getIndexWriter() {
		return indexWriter;
		
	}
}
