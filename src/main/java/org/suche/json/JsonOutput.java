package org.suche.json;

import java.io.IOException;

public interface JsonOutput {
	/**
	 *
	 * @return json string
	 */
	String toJson();
	/**
	 *
	 * @param buf StringBuilder
	 * @return filled StringBuilder
	 */
	default void toJson(Appendable buf) { try { buf.append(toJson()); } catch(IOException e) { throw new IllegalStateException(e); } }
}
