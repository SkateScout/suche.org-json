package org.suche.json;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class TransitionMapCtx {
	static Map<Class<?>, Map<String, KeyValueObject>> CACHE = new ConcurrentHashMap<>();

	static Map<String, KeyValueObject> getMeta(final Class<?> clazz) {
		var meta = CACHE.get(clazz);
		if (meta != null) return meta;
		final var arr = KeyValueObject.registerComplex(clazz, MetaConfig.DEFAULT);
		final var map = java.util.HashMap.<String, KeyValueObject>newHashMap(arr.length);
		for (final var kvo : arr) {
			final var bytes = kvo.jsonFirstKeyBytes();
			map.put(new String(bytes, 1, bytes.length - 3, StandardCharsets.UTF_8), kvo);
		}
		meta = Map.copyOf(map);
		CACHE.putIfAbsent(clazz, meta);
		return meta;
	}
}

public interface TransitionMap extends Map<String, Object> {
	@Override default int         size()                            { return TransitionMapCtx.getMeta(getClass()).size(); }
	@Override default boolean     isEmpty()                         { return TransitionMapCtx.getMeta(getClass()).isEmpty(); }
	@Override default boolean     containsKey  (final Object key  ) { return key instanceof final String k && TransitionMapCtx.getMeta(getClass()).containsKey(k); }
	@Override default boolean     containsValue(final Object value) { return values().contains(value); }
	@Override default Object      put   (final String key, final Object value) { throw new UnsupportedOperationException(); }
	@Override default Object      remove(final Object key)          { throw new UnsupportedOperationException(); }
	@Override default void        putAll(final Map<? extends String, ?> m)  { throw new UnsupportedOperationException(); }
	@Override default void        clear()                           { throw new UnsupportedOperationException(); }
	@Override default Set<String> keySet() { return TransitionMapCtx.getMeta(getClass()).keySet(); }

	@Override default Object  get(final Object key) {
		if (!(key instanceof final String strKey)) return null;
		final var kvo = TransitionMapCtx.getMeta(getClass()).get(strKey);
		if (kvo == null) return null;
		return switch (kvo.type()) {
		case 1  -> kvo.intGetter   ().applyAsInt   (this);
		case 2  -> kvo.longGetter  ().applyAsLong  (this);
		case 3  -> kvo.doubleGetter().applyAsDouble(this);
		case 4  -> kvo.boolGetter  ().test         (this);
		default -> kvo.objGetter   ().apply        (this);
		};
	}

	@Override default Collection<Object> values() {
		return new java.util.AbstractCollection<>() {
			@Override public Iterator<Object> iterator() { return entrySet().stream().map(Entry::getValue).iterator(); }
			@Override public int size() { return TransitionMap.this.size(); }
		};
	}

	@Override default Set<Entry<String, Object>> entrySet() {
		return new AbstractSet<>() {
			@Override public Iterator<Entry<String, Object>> iterator() {
				final var it = TransitionMapCtx.getMeta(getClass()).entrySet().iterator();
				return new Iterator<>() {
					@Override public boolean hasNext() { return it.hasNext(); }
					@Override public Entry<String, Object> next() {
						final var entry = it.next();
						return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), TransitionMap.this.get(entry.getKey()));
					}
				};
			}
			@Override public int size() { return TransitionMap.this.size(); }
		};
	}
}