package org.suche.json;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.suche.json.JsonOutputStream.Flags;
import org.suche.json.JsonOutputStream.TimeFormat;

final class EngineImpl implements InternalEngine {
	private static final int MOG_IGNORE = Modifier.INTERFACE | Modifier.ABSTRACT;

	private static class TypeRecord {
		final Class<?> clazz;
		final Object mutex = new Object();
		ObjectMeta metaObject;
		KeyValueObject[] kv;
		// MethodHandle filter;
		UnaryOperator<Object> transformer;
		int cacheIndex = -1;
		boolean building = false; // Prevents circular dependency infinite loops
		public TypeRecord(final Class<?> cls) { this.clazz = cls; }
		@Override public String toString() { return "TypeRecord [clazz=" + clazz + "]"; }

	}

	private boolean hasCoreTransformer = false;
	private boolean skipInvalid = false;
	private boolean failOnUnknownProperties = true;

	private final ConcurrentHashMap<Class<?>, TypeRecord> typeRecordCache = new ConcurrentHashMap<>();
	private TypeRecord getTypeRecord(final Class<?> c) { return typeRecordCache.computeIfAbsent(c, TypeRecord::new); }

	void putMeta(final Class<?> c, final ObjectMeta m) {
		final var t = getTypeRecord(c);
		t.metaObject = m;
		t.cacheIndex = m.cacheIndex;
	}

	private final ObjectMeta[] metaCache = new ObjectMeta[32768];
	private int metaCacheSize = ObjectMeta.IDX_CUSTOM_START;

	private final ObjectMeta defaultMapMeta       ; // -> ObjectMeta.IDX_MAP
	private final ObjectMeta genericCollectionMeta;
	private final ObjectMeta genericArrayMeta;
	private final ObjectMeta genericSetMeta;
	final MetaConfig cfg;

	@Override public ObjectMeta[] metaCache() { return metaCache; }
	@Override public void    skipInvalid(final boolean v) { this.skipInvalid = v; }
	@Override public boolean skipInvalid() { return skipInvalid; }
	@Override public void    failOnUnknownProperties(final boolean v) { this.failOnUnknownProperties = v; }
	@Override public boolean failOnUnknownProperties() { return failOnUnknownProperties; }

	final Collection<Predicate<Class<?>>> AUTO_POJO = new CopyOnWriteArrayList<>();
	@Override public void autoPojo(final Predicate<Class<?>> p) { AUTO_POJO.add(p); }

	// Used by metaIdOf getDynamicMetaId(Synchronized)
	synchronized int registerMeta(final ObjectMeta meta) {
		if (meta != null && meta.cacheIndex >= 0) return meta.cacheIndex;
		if (metaCacheSize >= metaCache.length) throw new IllegalStateException();
		final var index = metaCacheSize++;
		if (meta != null) metaCache[index] = meta;
		return index;
	}

	EngineImpl(final MetaConfig config) {
		this.cfg = config;
		this.defaultMapMeta        = new ObjectMeta(this, Map.class, ObjectMeta.IDX_MAP);
		this.genericCollectionMeta = new ObjectMeta(this, Object.class, ObjectMeta.TYPE_COLLECTION, ObjectMeta.IDX_COLLECTION);
		this.genericArrayMeta      = new ObjectMeta(this, Object.class, ObjectMeta.TYPE_OBJ_ARRAY, ObjectMeta.IDX_OBJ_ARRAY);
		this.genericSetMeta        = new ObjectMeta(this, Object.class, ObjectMeta.TYPE_SET, ObjectMeta.IDX_SET);

		metaCache[ObjectMeta.IDX_MAP       ] = defaultMapMeta;
		metaCache[ObjectMeta.IDX_COLLECTION] = genericCollectionMeta;
		metaCache[ObjectMeta.IDX_OBJ_ARRAY ] = genericArrayMeta;
		metaCache[ObjectMeta.IDX_SET       ] = genericSetMeta;

		putMeta(Object.class, defaultMapMeta);
		putMeta(Map.class, defaultMapMeta);
	}

	static long createTypeDesc(final boolean isArray, final boolean isPrimitive, final int cacheIndex) {
		var desc = ((long) cacheIndex) << 1;
		if (isArray) desc |= 0x8000000000000000L;
		if (isPrimitive) desc |= 1L;
		return desc;
	}

	private final ObjectMeta[] dynamicMetaCache = new ObjectMeta[32];
	private int dynamicMetaCount = 0;

	// Not used in hot path only by resolveDescriptor in Constructor
	synchronized int getDynamicMetaId(final Class<?> compType, final int targetMetaType) {
		final var searchType = compType == null ? Object.class : compType;
		if (searchType == Object.class) {
			return switch (targetMetaType) {
			case ObjectMeta.TYPE_COLLECTION -> ObjectMeta.IDX_COLLECTION;
			case ObjectMeta.TYPE_OBJ_ARRAY  -> ObjectMeta.IDX_OBJ_ARRAY;
			case ObjectMeta.TYPE_SET        -> ObjectMeta.IDX_SET;
			case ObjectMeta.TYPE_MAP        -> ObjectMeta.IDX_MAP;
			default                         -> ObjectMeta.IDX_GENERIC;
			};
		}
		for (var i = 0; i < dynamicMetaCount; i++) {
			if (dynamicMetaCache[i].metaType == targetMetaType && dynamicMetaCache[i].components[0].type() == searchType) return dynamicMetaCache[i].cacheIndex;
		}
		final var m = new ObjectMeta(this, searchType, targetMetaType, registerMeta(null));
		metaCache[m.cacheIndex] = m;
		if (dynamicMetaCount < dynamicMetaCache.length) dynamicMetaCache[dynamicMetaCount++] = m;
		return m.cacheIndex;
	}

	int metaIdOf(final Class<?> clazz) {
		if (clazz == null || clazz == Object.class || Map.class.isAssignableFrom(clazz)) return ObjectMeta.IDX_MAP;

		final var t = getTypeRecord(clazz);
		if (t.cacheIndex == -1) {
			synchronized (t.mutex) {
				if (t.cacheIndex == -1) t.cacheIndex = registerMeta(null);
			}
		}
		if (t.metaObject != null) return t.cacheIndex;

		synchronized (t.mutex) {
			// Circular reference detected! Returning the ID early is sufficient for linking.
			if ((t.metaObject != null) || t.building) return t.cacheIndex;

			t.building = true;
			final ObjectMeta r;
			if (clazz.isPrimitive() || clazz.isArray() || 0 != (clazz.getModifiers() & MOG_IGNORE)) r = ObjectMeta.NULL;
			else if (clazz.isRecord()) r = ObjectMeta.ofRecord(this, clazz.asSubclass(Record.class), t.cacheIndex);
			else if (clazz.isEnum()) r = ObjectMeta.ofEnum(this, clazz.asSubclass(Enum.class), t.cacheIndex);
			else r = ObjectMeta.ofPojo(this, clazz, t.cacheIndex);

			t.metaObject = r;
			if (r != ObjectMeta.NULL) {
				metaCache[t.cacheIndex] = r;
			}
			t.building = false;
			return t.cacheIndex;
		}
	}

	@Override public ObjectMeta metaOf(final Class<?> clazz) {
		final var id = metaIdOf(clazz);
		if (id == ObjectMeta.IDX_GENERIC || metaCache[id] == null) return null;
		return metaCache[id];
	}

	@Override public JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final Flags... flags) { return JsonOutputStream.of(this, out, timeFormat, flags); }
	@Override public JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final int flags) { return JsonOutputStream.of(this, out, timeFormat, flags); }
	@Override public JsonOutputStream jsonOutputStream(final OutputStream out) { return JsonOutputStream.of(this, out, null, 0); }
	@Override public UnaryOperator<Object> transformer(final Class<?> c) { return getTypeRecord(c).transformer; }
	@Override public boolean hasCoreTransformer() { return hasCoreTransformer; }
	@Override public <C> void registerTransformer(final Class<C> c, final UnaryOperator<Object> f) { getTypeRecord(c).transformer = f; if (c.getClassLoader() == null || c.isArray()) hasCoreTransformer = true; }
	@Override public JsonInputStream jsonInputStream(final InputStream is) { return JsonInputStream.of(is, this); }
	@Override public MetaConfig config() { return cfg; }

	// @Override public MethodHandle getFilter(final Class<?> c) {
	// 	final var t = getTypeRecord(c);
	// 	var m = t.filter;
	// 	if (m == null) {
	// 		m = Meta.newFilter(c);
	// 		if (m == null) m = MethodHandles.identity(Object.class);
	// 		t.filter = m;
	// 	}
	// 	return m;
	// }

	@Override
	public KeyValueObject[] ofComplex(final Class<?> c) {
		final var t = getTypeRecord(c);
		if (t.kv != null) return t.kv == KeyValueObject.NULL ? null : t.kv;
		synchronized (t.mutex) {
			if (t.kv != null) return t.kv == KeyValueObject.NULL ? null : t.kv;
			KeyValueObject[] rp = null;
			if (Record.class.isAssignableFrom(c)) rp = KeyValueObject.ofRecord(c.asSubclass(Record.class), cfg);
			else for (final var p : AUTO_POJO) if (p.test(c)) rp = KeyValueObject.registerComplex(c, cfg);
			t.kv = rp == null ? KeyValueObject.NULL : rp;
			return rp;
		}
	}
}