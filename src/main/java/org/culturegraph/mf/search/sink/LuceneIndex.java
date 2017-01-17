package org.culturegraph.mf.search.sink;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.NIOFSDirectory;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.helpers.DefaultStreamReceiver;
import org.culturegraph.mf.metamorph.Metamorph;
import org.culturegraph.mf.search.index.IndexException;
import org.culturegraph.mf.search.index.StreamIndexer;
import org.culturegraph.mf.search.index.StreamIndexerBuilder;
import org.culturegraph.mf.search.index.TextKeywordsMixedAnalyzerFactory;

@Description("writes to a lucene index")
@In(StreamReceiver.class)
public final class LuceneIndex extends DefaultStreamReceiver {

	private static final int DEFAULT_BATCH_SIZE = 10000;
	private static final int DEFAULT_RAM_BUFFER = 200;

	private int ramBuffer = DEFAULT_RAM_BUFFER;
	private int batchSize = DEFAULT_BATCH_SIZE;

	private String indexPath = "index";
	private final String morphDef;
	private boolean init;
	private StreamIndexer streamIndexer;

	public void setBatchSize(final int batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * @param ramBuffer in MB
	 */
	public void setRamBuffer(int ramBuffer) {
		this.ramBuffer = ramBuffer;
	}

	public LuceneIndex(final String morphDef) {
		super();
		this.morphDef = morphDef;
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
		streamIndexer.startRecord(identifier);
	}

	@Override
	public void endRecord() {
		streamIndexer.endRecord();
	}

	@Override
	public void startEntity(final String name) {
		streamIndexer.startEntity(name);
	}

	@Override
	public void endEntity() {
		streamIndexer.endEntity();
	}

	@Override
	public void literal(final String name, final String value) {
		streamIndexer.literal(name, value);
	}

	@Override
	public void resetStream() {
		init = false;
	}

	@Override
	public void closeStream() {
		streamIndexer.closeStream();
	}

	private void initIndex() throws IOException {
		final Metamorph morph = new Metamorph(morphDef);
		final Map<String,String> textfields = morph.getMap("textfields");
		final Set<String> analyzedFields;
		if(null==textfields){
			analyzedFields = Collections.emptySet();
		}else{
			analyzedFields = textfields.keySet();
		}
		final Analyzer analyzer = new TextKeywordsMixedAnalyzerFactory(analyzedFields).create();

		streamIndexer = StreamIndexerBuilder.build(new NIOFSDirectory(new File(indexPath)), ramBuffer, batchSize, analyzer, morph);
		streamIndexer.getIndexWriter().setInfoStream(System.err);
		init = true;
	}

	public void setIndexPath(final String indexName) {
		this.indexPath = indexName;
	}

}
