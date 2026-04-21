package org.suche.json;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public final class CompactMap<K, V> extends AbstractMap<K, V> {
	private final Object[] data;
	private Set<Map.Entry<K, V>> entrySet;

	Object[] getRawData() { return data; }


	CompactMap(final Object[] data) { this.data = data; }
	@Override public int size() { return data.length >> 1; }

	@Override @SuppressWarnings("unchecked") public V get(final Object key) {
		for (var i = 0; i < data.length - 1; i += 2) if (key.equals(data[i])) return (V) data[i + 1];
		return null;
	}

	@Override public boolean containsKey(final Object key) {
		for (var i = 0; i < data.length; i += 2) if (key.equals(data[i])) return true;
		return false;
	}

	@Override public Set<Map.Entry<K, V>> entrySet() {
		if (entrySet == null) {
			entrySet = new AbstractSet<>() {
				@Override public int size() { return data.length >> 1; }
				@Override public Iterator<Map.Entry<K, V>> iterator() {
					return new Iterator<>() {
						private int index = 0;
						@Override public boolean hasNext() { return index < data.length; }
						@Override @SuppressWarnings("unchecked") public Map.Entry<K, V> next() {
							if (index >= data.length - 1) throw new NoSuchElementException();
							final var k = (K) data[index++];
							final var v = (V) data[index++];
							return new java.util.AbstractMap.SimpleImmutableEntry<>(k, v);
						}
					};
				}
			};
		}
		return entrySet;
	}

	@Override
	public void forEach(final java.util.function.BiConsumer<? super K, ? super V> action) {
		if (action == null) throw new NullPointerException();
		for (var i = 0; i < data.length - 1; i += 2) {
			@SuppressWarnings("unchecked")
			final var k = (K) data[i];
			@SuppressWarnings("unchecked")
			final var v = (V) data[i + 1];
			action.accept(k, v);
		}
	}
}