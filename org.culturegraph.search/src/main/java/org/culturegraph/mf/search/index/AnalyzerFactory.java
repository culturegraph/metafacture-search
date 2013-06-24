package org.culturegraph.mf.search.index;

import org.apache.lucene.analysis.Analyzer;

public interface AnalyzerFactory {
	Analyzer create();
}
