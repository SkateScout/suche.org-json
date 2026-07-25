package org.suche.json;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

final class EmptyJSONArray implements JSONArray {
	static final EmptyJSONArray ONCE = new EmptyJSONArray();
	private EmptyJSONArray() { }
	private static final Object[] EMPTY = { };

	private static NoSuchElementException noSuchElementException() { throw new NoSuchElementException(); }
	private static UnsupportedOperationException readOnly() { throw new UnsupportedOperationException("ReadOnly"); }

	private static final ListIterator<Object> EMPTY_I = new ListIterator<>() {
		@Override public boolean hasNext() { return false; }
		@Override public Object next() { throw noSuchElementException(); }
		@Override public boolean hasPrevious() { return false; }
		@Override public Object previous() { throw noSuchElementException(); }
		@Override public int nextIndex() { return 0; }
		@Override public int previousIndex() { return -1; }
		@Override public void remove() { /* Nothing to remove from empty list */ }
		@Override public void set(final Object e) { throw readOnly(); }
		@Override public void add(final Object e) { throw readOnly(); }
	};

	@Override public long[] prims() { return CompactList.NO_PRIMS; }
	@Override public byte singleType() { return PRIMITIVE.T_EMPTY; }
	@Override public Object rawValueAt(final int logicalIdx) { return null; }
	@Override public int size() { return 0; }
	@Override public boolean isEmpty() { return true; }
	@Override public boolean contains(final Object o) { return false; }
	@Override public Iterator<Object> iterator() { return EMPTY_I; }
	@Override public Object[] toArray() { return EMPTY; }
	@Override public <T> T[] toArray(final T[] a) { return a; }
	@Override public boolean add   (final Object e)                                        { throw readOnly(); }
	@Override public boolean addAll(final Collection<? extends Object> c)                  { throw readOnly(); }
	@Override public boolean addAll(final int index, final Collection<? extends Object> c) { throw readOnly(); }
	@Override public Object  get   (final int index)                                       { throw readOnly(); }
	@Override public Object  set   (final int index, final Object element)                 { throw readOnly(); }
	@Override public void    add   (final int index, final Object element)                 { throw readOnly(); }
	@Override public Object  remove(final int index)                                       { throw readOnly(); }
	@Override public boolean remove(final Object o) { return false; }
	@Override public boolean containsAll(final Collection<?> c) { return false; }
	@Override public boolean removeAll(final Collection<?> c) { return false; }
	@Override public boolean retainAll(final Collection<?> c) { return false; }
	@Override public void clear() { /* Nothing to remove from empty list */ }
	@Override public int indexOf(final Object o) { return -1; }
	@Override public int lastIndexOf(final Object o) { return -1; }
	@Override public ListIterator<Object> listIterator() { return EMPTY_I; }
	@Override public ListIterator<Object> listIterator(final int index) { return EMPTY_I; }
	@Override public List<Object> subList(final int fromIndex, final int toIndex) { return this; }
	@Override public int length() { return 0; }
	@Override public JSONObject getJSONObject(final int index) { throw noSuchElementException(); }
	@Override public JSONObject optJSONObject(final int index) { throw noSuchElementException(); }
	@Override public JSONArray  getJSONArray (final int index) { throw noSuchElementException(); }
	@Override public JSONArray  optJSONArray (final int index) { throw noSuchElementException(); }
	@Override public String     getString    (final int index) { throw noSuchElementException(); }
	@Override public String     optString    (final int index, final String fallback) { throw noSuchElementException(); }
	@Override public void put(final int idx, final Object val) { throw noSuchElementException(); }
	@Override public void removeByIndex(final int index) { throw noSuchElementException(); }
}