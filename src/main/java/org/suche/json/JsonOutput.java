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
	default void toJson(final Appendable buf) { try { buf.append(toJson()); } catch(final IOException e) { throw new IllegalStateException(e); } }
}

/*

Neuer Serializer

case TYPE_RECORD -> {
    final var serializer = (ObjectSerializer) c.meta;
    c.idx = serializer.serialize(this, c.obj, c.idx);

    if (c.idx == -1) {
        nextVal = EXHAUSTED;
    } else {
        nextVal = this.queuedComplex;
        this.queuedComplex = null; // Clean up
        break; // Bricht das Switch ab, äußere Schleife verarbeitet nextVal
    }
}



 */
