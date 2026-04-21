package org.suche.json;

import java.util.AbstractList;
import java.util.RandomAccess;

public final class CompactList<E> extends AbstractList<E> implements RandomAccess {
	private final Object[] data;
	Object[] getRawData() { return data; }
	CompactList(final Object[] data) { this.data = data; }

	@Override
	@SuppressWarnings("unchecked")
	public E get(final int index) { return (E) data[index]; }

	@Override public int size() { return data.length; }
}