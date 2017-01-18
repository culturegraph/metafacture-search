package org.culturegraph.mf.search.index;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.culturegraph.mf.formeta.FormetaEncoder;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.helpers.DefaultObjectReceiver;
import org.culturegraph.mf.framework.helpers.DefaultStreamReceiver;
import org.culturegraph.mf.metamorph.Metamorph;
import org.culturegraph.mf.plumbing.StreamTee;
import org.culturegraph.mf.search.IndexConstants;

/**
 * Writes an event stream to a Lucene Index.
 *
 * @author Markus Michael Geipel
 */
public final class StreamIndexer implements StreamReceiver {

	private final BatchIndexer indexer;
	private final StreamTee tee = new StreamTee();

	public StreamIndexer(final IndexWriter indexWriter) {
		indexer = new BatchIndexer(indexWriter);
		tee.addReceiver(new IndexedFieldReceiver(indexer));
		final FormetaEncoder encoder = new FormetaEncoder();
		tee.addReceiver(encoder);
		encoder.setReceiver(new SerializedFieldReceiver(indexer));
	}


	public StreamIndexer(final IndexWriter indexWriter,
			final Metamorph metamorph) {
		indexer = new BatchIndexer(indexWriter);
		tee.addReceiver(metamorph);
		metamorph.setReceiver(new IndexedFieldReceiver(indexer));
		final FormetaEncoder encoder = new FormetaEncoder();
		tee.addReceiver(encoder);
		encoder.setReceiver(new SerializedFieldReceiver(indexer));

	}

	public IndexWriter getIndexWriter() {
		return indexer.getIndexWriter();
	}

	public int getCount() {
		return indexer.getCount();
	}

	@Override
	public void startRecord(final String identifier) {
		indexer.startDocument(identifier);
		tee.startRecord(identifier);
	}

	@Override
	public void endRecord() {
		tee.endRecord();
		indexer.endDocument();
	}

	@Override
	public void startEntity(final String name) {
		tee.startEntity(name);
	}

	@Override
	public void endEntity() {
		tee.endEntity();
	}

	@Override
	public void literal(final String name, final String value) {
		tee.literal(name, value);
	}

	@Override
	public void resetStream() {
		throw new UnsupportedOperationException("Cannot reset StreamIndexer");
	}

	@Override
	public void closeStream() {
		indexer.flush();
		indexer.close();
	}

	public int getBatchSize() {
		return indexer.getBatchSize();
	}

	public void setBatchSize(final int batchSize) {
		indexer.setBatchSize(batchSize);
	}

	private static final class IndexedFieldReceiver extends DefaultStreamReceiver {
		private final BatchIndexer indexer;

		public IndexedFieldReceiver(final BatchIndexer indexer) {
			super();
			this.indexer = indexer;
		}

		@Override
		public void literal(final String name, final String value) {
			indexer.add(new Field(name, value, Field.Store.NO, Field.Index.ANALYZED));
		}
	}

	private static final class SerializedFieldReceiver
			extends DefaultObjectReceiver<String> {

		private final BatchIndexer indexer;

		public SerializedFieldReceiver(final BatchIndexer indexer) {
			super();
			this.indexer = indexer;
		}

		@Override
		public void process(final String value) {
			indexer.add(new Field(IndexConstants.SERIALIZED, value,
					Field.Store.YES, Field.Index.NO));
		}

	}

}
