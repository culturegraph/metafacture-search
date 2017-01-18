package org.culturegraph.mf.search.sink;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.helpers.DefaultStreamReceiver;
import org.culturegraph.mf.search.index.BatchIndexer;
import org.culturegraph.mf.search.index.IndexException;

@Description("writes to a simple lucene index without analysis or stored fields")
@In(StreamReceiver.class)
public final class PrimaryKeyLuceneIndex extends DefaultStreamReceiver {

	private String indexPath = "index";
	private boolean init;
	private int batchSize = 10000;
	private int ramBuffer = 200;
	private BatchIndexer indexer;

	public PrimaryKeyLuceneIndex() {
		super();
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public void setRamBuffer(int ramBuffer) {
		this.ramBuffer = ramBuffer;
	}

	@Override
	public void startRecord(final String identifier) {
		if (!init) {
			try {
				initIndex();
			} catch (IOException e) {
				throw new IndexException("error opening index", e);
			}
		}
		indexer.startDocument(identifier);
	}

	@Override
	public void endRecord() {
		indexer.endDocument();
	}

	@Override
	public void literal(final String name, final String value) {
		indexer.add(new Field(name, value, Field.Store.YES,
				Field.Index.NOT_ANALYZED_NO_NORMS));
	}

	@Override
	public void resetStream() {
		init = false;
	}

	@Override
	public void closeStream() {
		indexer.flush();
		indexer.close();
	}

	private void initIndex() throws IOException {
		try {
			final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(
					Version.LUCENE_36, new KeywordAnalyzer());
			indexWriterConfig.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
			indexWriterConfig.setRAMBufferSizeMB(ramBuffer);
			indexWriterConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);

			final IndexWriter indexWriter = new IndexWriter(new NIOFSDirectory(
					new File(indexPath)), indexWriterConfig);
			indexWriter.setInfoStream(System.err);
			indexer = new BatchIndexer(indexWriter);
			indexer.setBatchSize(batchSize);

		} catch (NumberFormatException e) {
			throw new IndexException("Error in indexer properties. Not a number: " +
					e.getMessage(), e);
		}
		init = true;
	}

	public void setIndexPath(final String indexName) {
		this.indexPath = indexName;
	}

}
