package org.culturegraph.mf.search.index;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.culturegraph.mf.metamorph.Metamorph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamIndexerBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIndexerBuilder.class);

	private StreamIndexerBuilder() {
		throw new AssertionError("No instances allowed");
	}

	public static StreamIndexer build(final Directory directory,
			final int ramBuffer, final int batchSize,
			final Analyzer analyzer, final Metamorph morph) throws IOException {
		if (directory == null) {
			throw new IllegalArgumentException("'directory' must not be null");
		}

		if (analyzer == null) {
			throw new IllegalArgumentException("'analyzer' must not be null");
		}

		try {
			final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(
					Version.LUCENE_36, analyzer);
			indexWriterConfig.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);

			LOG.info("Indexer Ram buffer: " + ramBuffer + "mb");
			indexWriterConfig.setRAMBufferSizeMB(ramBuffer);
			indexWriterConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);

			final IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);

			final StreamIndexer streamIndexer;
			if (null == morph) {
				streamIndexer = new StreamIndexer(indexWriter);
			} else {
				streamIndexer = new StreamIndexer(indexWriter, morph);
			}
			LOG.info("Batch size: " + batchSize);
			streamIndexer.setBatchSize(batchSize);
			return streamIndexer;
		} catch (NumberFormatException e) {
			throw new IndexException("Error in indexer properties. Not a number: " +
					e.getMessage(), e);
		}
	}

}
