package org.suche.json;

public class JSONException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	final Throwable cause;

	JSONException(final String mesg) {
		super(mesg);
		cause = null;
	}

	JSONException(final Throwable t) {
		super(t.getMessage(), null, false, false);
		cause = t;
		setStackTrace(t.getStackTrace());
	}
}