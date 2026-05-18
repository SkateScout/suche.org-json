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
	private static final ListIterator<Object> EMPTY_I = new ListIterator<>() {
		@Override public boolean hasNext() { return false; }
		@Override public Object next() { throw new NoSuchElementException(); }
		@Override public boolean hasPrevious() { return false; }
		@Override public Object previous() { throw new NoSuchElementException(); }
		@Override public int nextIndex() { return 0; }
		@Override public int previousIndex() { return -1; }
		@Override public void remove() { }
		@Override public void set(final Object e) { throw new IllegalStateException("Read Only"); }
		@Override public void add(final Object e) { throw new IllegalStateException("Read Only"); }
	};

	@Override public long[] prims() { return null; }
	@Override public byte singleType() { return PRIMITIVE.T_EMPTY; }
	@Override public Object rawValueAt(final int logicalIdx) { return null; }
	@Override public int size() { return 0; }
	@Override public boolean isEmpty() { return true; }
	@Override public boolean contains(final Object o) { return false; }
	@Override public Iterator<Object> iterator() { return EMPTY_I; }
	@Override public Object[] toArray() { return EMPTY; }
	@Override public <T> T[] toArray(final T[] a) { return a; }
	@Override public boolean add(final Object e) { throw new UnsupportedOperationException("ReadOnly"); }
	@Override public boolean remove(final Object o) { return false; }
	@Override public boolean containsAll(final Collection<?> c) { return false; }
	@Override public boolean addAll(final Collection<? extends Object> c) { throw new UnsupportedOperationException("ReadOnly"); }
	@Override public boolean addAll(final int index, final Collection<? extends Object> c) { throw new UnsupportedOperationException("ReadOnly"); }
	@Override public boolean removeAll(final Collection<?> c) { return false; }
	@Override public boolean retainAll(final Collection<?> c) { return false; }
	@Override public void clear() { }
	@Override public Object get(final int index) { throw new UnsupportedOperationException("ReadOnly"); }
	@Override public Object set(final int index, final Object element) { throw new UnsupportedOperationException("ReadOnly"); }
	@Override public void add(final int index, final Object element) { throw new UnsupportedOperationException("ReadOnly"); }
	@Override public Object remove(final int index) { throw new UnsupportedOperationException("ReadOnly"); }
	@Override public int indexOf(final Object o) { return -1; }
	@Override public int lastIndexOf(final Object o) { return -1; }
	@Override public ListIterator<Object> listIterator() { return EMPTY_I; }
	@Override public ListIterator<Object> listIterator(final int index) { return EMPTY_I; }
	@Override public List<Object> subList(final int fromIndex, final int toIndex) { return this; }
	@Override public int length() { return 0; }
	@Override public JSONObject getJSONObject(final int index) { throw new NoSuchElementException(); }
	@Override public JSONObject optJSONObject(final int index) { throw new NoSuchElementException(); }
	@Override public JSONArray getJSONArray(final int index) { throw new NoSuchElementException(); }
	@Override public JSONArray optJSONArray(final int index) { throw new NoSuchElementException(); }
	@Override public String getString(final int index) { throw new NoSuchElementException(); }
	@Override public String optString(final int index, final String fallback) { throw new NoSuchElementException(); }
	@Override public void put(final int idx, final int val) { throw new NoSuchElementException(); }
	@Override public void removeByIndex(final int index) { throw new NoSuchElementException(); }
}