package org.culturegraph.mf.search.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

/**
 * Factory for {@link PerFieldAnalyzerWrapper} which uses
 * {@link StandardAnalyzer} for textFields and {@link KeywordAnalyzer} for all
 * other fields.
 *
 * @author Markus Michael Geipel
 */
public final class TextKeywordsMixedAnalyzerFactory implements AnalyzerFactory {

	private Set<String> textFields = new HashSet<String>();

	public TextKeywordsMixedAnalyzerFactory(final String... textFields) {
		setTextFields(textFields);
	}

	public TextKeywordsMixedAnalyzerFactory(final Set<String> textFields) {
		setTextFields(textFields);
	}


	public TextKeywordsMixedAnalyzerFactory() {
		// nothing
	}

	@Override
	public Analyzer create() {
		final Map<String, Analyzer> map = new HashMap<String, Analyzer>();
		for (String field : textFields) {
			map.put(field, new GermanAnalyzer(Version.LUCENE_36));
		}
		return new PerFieldAnalyzerWrapper(new KeywordAnalyzer(), map);
	}

	public void setTextFields(final String... textFields) {
		this.textFields.clear();
		this.textFields.addAll(Arrays.asList(textFields));

	}

	private void setTextFields(final Set<String> textFields) {
		this.textFields = textFields;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " with textfileds " + textFields;
	}

}
