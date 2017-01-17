package org.culturegraph.mf.search.index;

import org.culturegraph.mf.framework.MetafactureException;

public class IndexException extends MetafactureException {
	private static final long serialVersionUID = -3510759004189481875L;

	public IndexException(final String message) {
		super(message);
	}

	public IndexException(final Throwable cause) {
		super(cause);
	}

	public IndexException(final String message, final Throwable cause) {
		super(message, cause);
	}

}
