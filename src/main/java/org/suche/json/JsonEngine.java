package org.suche.json;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;

import org.suche.json.JsonOutputStream.Flags;
import org.suche.json.JsonOutputStream.TimeFormat;

public sealed interface JsonEngine permits InternalEngine {
	JsonInputStream jsonInputStream(InputStream is);
	static JsonEngine of (final MetaConfig cfg) { return new EngineImpl(cfg); }

	JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final Flags... flags);
	JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final int flags);
	JsonOutputStream jsonOutputStream(final OutputStream out);

	<C> void registerTransformer(final Class<C> c, final Function<C,Object> f);
	void maxRecursiveDepth(int v);

	void skipInvalid(final boolean v);
	boolean skipInvalid();

	void failOnUnknownProperties(final boolean v);
	boolean failOnUnknownProperties();

	void autoPojo(final Predicate<Class<?>> p);
}

sealed interface InternalEngine extends JsonEngine permits EngineImpl {
	ObjectMeta       metaOf   (Class<?> clazz);
	MetaConfig       config   ();
	MethodHandle     getFilter(final Class<?> c);
	KeyValueObject[] ofComplex( final Class<?> c);
	Map<Class<?>, Function<Object,Object>> transformers();
	boolean                                hasCoreTransformer();
	int maxRecursiveDepth();
}

final class EngineImpl implements InternalEngine {
	private static final RuntimeException                       E_DEFEKT1          = new RuntimeException("DEFEKT-1", null, false, false) { };
	private static final int                                    MOG_IGNORE         = Modifier.INTERFACE | Modifier.ABSTRACT;
	static         final ObjectMeta                             DEFECT             = new ObjectMeta();
	private        final Map<Class<?>, ObjectMeta      >        metaObjectCache    = new ConcurrentHashMap<>();
	private        final Map<Class<?>, MethodHandle    >        filterCache        = new ConcurrentHashMap<>();
	private        final Map<Class<?>, Object          >        buildLocks         = new ConcurrentHashMap<>();
	private        final Map<Class<?>, KeyValueObject[]>        kvCache            = new ConcurrentHashMap<>();
	private        final Map<Class<?>, Function<Object,Object>> transformers       = new ConcurrentHashMap<>();
	private              boolean                                hasCoreTransformer = false;
	private              int                                    maxRecursiveDepth = 128;
	private              boolean                                skipInvalid = false;
	private              boolean                                failOnUnknownProperties = true;

	@Override public void maxRecursiveDepth(final int v) { this.maxRecursiveDepth = v; }
	@Override public int maxRecursiveDepth() { return maxRecursiveDepth; }

	@Override public void skipInvalid(final boolean v) { this.skipInvalid = v; }
	@Override public boolean skipInvalid() { return skipInvalid; }

	@Override public void failOnUnknownProperties(final boolean v) { this.failOnUnknownProperties = v; }
	@Override public boolean failOnUnknownProperties() { return failOnUnknownProperties; }

	final Collection<Predicate<Class<?>>> AUTO_POJO = new CopyOnWriteArrayList<>();
	@Override public void autoPojo(final Predicate<Class<?>> p) { AUTO_POJO.add(p); }

	final MetaConfig cfg;

	public EngineImpl(final MetaConfig cfg) {
		this.cfg = cfg;
		metaObjectCache.put(Object       .class, ObjectMeta.GENERIC_MAP);
		metaObjectCache.put(java.util.Map.class, ObjectMeta.GENERIC_MAP);
		metaObjectCache.put(CompactMap   .class, ObjectMeta.GENERIC_MAP);
		metaObjectCache.put(AbstractMap  .class, ObjectMeta.GENERIC_MAP);
		metaObjectCache.put(String       .class, ObjectMeta.NULL);
		metaObjectCache.put(Boolean      .class, ObjectMeta.NULL);
		metaObjectCache.put(CompactMap   .class, ObjectMeta.NULL);
		metaObjectCache.put(Byte         .class, ObjectMeta.NULL);
		metaObjectCache.put(Short        .class, ObjectMeta.NULL);
		metaObjectCache.put(Integer      .class, ObjectMeta.NULL);
		metaObjectCache.put(Long         .class, ObjectMeta.NULL);
		metaObjectCache.put(Float        .class, ObjectMeta.NULL);
		metaObjectCache.put(Double       .class, ObjectMeta.NULL);
	}

	@Override public JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final Flags... flags)  {
		return JsonOutputStream.of(this, out, timeFormat, flags);
	}

	@Override public JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final int flags)  {
		return JsonOutputStream.of(this, out, timeFormat, flags);
	}

	@Override public JsonOutputStream jsonOutputStream(final OutputStream out) { return JsonOutputStream.of(this, out, null, 0); }

	@Override public Map<Class<?>, Function<Object,Object>> transformers      () { return transformers      ; }
	@Override public boolean                                hasCoreTransformer() { return hasCoreTransformer; }

	@Override public <C> void registerTransformer(final Class<C> c, final Function<C,Object> f) {
		transformers.put(c, obj -> f.apply(c.cast(obj)));
		if (c.getClassLoader() == null || c.isArray()) hasCoreTransformer = true;
	}

	@Override public JsonInputStream jsonInputStream(final InputStream is) { return JsonInputStream.of(is, this); }
	@Override public MetaConfig      config         () { return cfg; }
	@Override public MethodHandle    getFilter(final Class<?> c) { return filterCache.computeIfAbsent(c, k->Meta.newFilter(c)); }

	private ObjectMeta createMeta(final Class<?> c) {
		if (c.isPrimitive() || c.isArray()) return ObjectMeta.NULL;
		final var modifiers = c.getModifiers();
		if (Number    .class.isAssignableFrom(c)) { System.err.println("ObjectMeta.of("+c.getCanonicalName()+") => Skip Number"); return ObjectMeta.NULL; }
		if (Collection.class.isAssignableFrom(c)) { System.err.println("ObjectMeta.of("+c.getCanonicalName()+") => Collection");  return ObjectMeta.NULL; }
		if (0 != (modifiers & MOG_IGNORE)) {
			if (c.getPermittedSubclasses() != null) return SealedUnionMapper.build(this, c);
			if (Modifier.isInterface(modifiers)) System.err.println("ObjectMeta.of("+c.getCanonicalName()+") => Skip Interface");
			if (Modifier.isAbstract (modifiers)) System.err.println("ObjectMeta.of("+c.getCanonicalName()+") => Skip Abstract");
			return ObjectMeta.NULL;
		}

		final var mutex = buildLocks.computeIfAbsent(c, _->new Object());
		try {
			synchronized (mutex) {
				if (metaObjectCache.get(c) instanceof final ObjectMeta r) {
					if (r == ObjectMeta.DEFECT_FIRST || r == DEFECT) throw ObjectMeta.E_DEFEKT2;
					return r == ObjectMeta.NULL ? null : r;
				}
				final ObjectMeta r;
				if     (c.isRecord()) r = ObjectMeta.ofRecord(this, c.asSubclass(Record.class));
				else if(c.isEnum  ()) r = ObjectMeta.ofEnum  (this, c.asSubclass(Enum  .class));
				else                  r = ObjectMeta.ofPojo  (this, c);
				metaObjectCache.put(c, r);
				return r;
			}
		} finally { buildLocks.remove(c, mutex);
		}
	}

	@Override public ObjectMeta metaOf(final Class<?> clazz) {
		if (clazz == null) return null;
		if (metaObjectCache.get(clazz) instanceof final ObjectMeta r) {
			if (r == ObjectMeta.DEFECT) throw E_DEFEKT1;
			if (r == ObjectMeta.DEFECT_FIRST) { metaObjectCache.put(clazz, ObjectMeta.DEFECT); throw new IllegalArgumentException("Clazz: " + clazz.getCanonicalName()); }
			return r == ObjectMeta.NULL ? null : r;
		}
		final var r = createMeta(clazz);
		if (r == ObjectMeta.NULL || r == null) { metaObjectCache.putIfAbsent(clazz, ObjectMeta.NULL); return null; }
		if (r == ObjectMeta.DEFECT_FIRST)  { metaObjectCache.put(clazz, ObjectMeta.DEFECT); throw new IllegalArgumentException("Clazz: " + clazz.getCanonicalName()); }
		metaObjectCache.putIfAbsent(clazz, r);
		return r;
	}

	private static boolean skipClass(final String cls) { return (cls.startsWith("java.") || cls.startsWith("javax.") || cls.startsWith("jdk.") || cls.startsWith("sun.")); }

	@Override
	@SuppressWarnings("unchecked") public KeyValueObject[] ofComplex( final Class<?> c) {
		if(kvCache.get(c) instanceof final KeyValueObject[] r) return r == KeyValueObject.NULL ? null : r;
		if (c.getClassLoader() == null || skipClass(c.getName())) { kvCache.put(c, KeyValueObject.NULL); return null; }
		final var emitClass = cfg.emitClassName();
		KeyValueObject[] rp = null;
		if(Record.class.isAssignableFrom(c))            rp =  KeyValueObject.ofRecord(c.asSubclass(Record.class), cfg);
		else for(final var p : AUTO_POJO) if(p.test(c)) rp =  KeyValueObject.registerComplex(c, cfg);
		if(rp == null) {
			final var once = new HashMap<>();
			for(final var e : kvCache.entrySet()) if(e.getKey().isAssignableFrom(c)) for(final var fld : e.getValue()) once.put(new String(fld.jsonKeyBytes(), StandardCharsets.UTF_8), fld);
			if(once.isEmpty() && !emitClass) { kvCache.put(c, KeyValueObject.NULL); return null; }
			rp = once.values().toArray(KeyValueObject.NULL);
		}
		if (rp.length == 0 && !emitClass) throw new IllegalArgumentException("Class "+c.getCanonicalName()+" without getter or fields");
		if(cfg.emitClassName()) {
			final var className = c.getSimpleName();
			final var re = new KeyValueObject[rp.length + 1];
			re[0] = new KeyValueObject(KeyValueObject.CLASS_NAME, 0, _ -> className, null, null, null, null);
			System.arraycopy(rp, 0, re, 1, rp.length);
			rp = re;
		}
		kvCache.put(c, rp);
		return rp;
	}
}