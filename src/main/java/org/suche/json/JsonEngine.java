package org.suche.json;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.suche.json.JsonOutputStream.Flags;
import org.suche.json.JsonOutputStream.TimeFormat;

public sealed interface JsonEngine permits InternalEngine {
	JsonInputStream jsonInputStream(InputStream is);
	static JsonEngine of (final MetaConfig cfg) { return new EngineImpl(cfg); }

	JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final Flags... flags);
	JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final int flags);
	JsonOutputStream jsonOutputStream(final OutputStream out);

	<C> void registerTransformer(final Class<C> c, final UnaryOperator<Object> f);
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
	UnaryOperator<Object> transformer(Class<?> c);
	boolean                 hasCoreTransformer();
	ObjectMeta[] metaCache();
	int maxRecursiveDepth();
}

final class EngineImpl implements InternalEngine {
	private static final RuntimeException                       E_DEFEKT1          = new RuntimeException("DEFEKT-1", null, false, false) { };
	private static final int                                    MOG_IGNORE         = Modifier.INTERFACE | Modifier.ABSTRACT;
	static         final ObjectMeta                             DEFECT             = new ObjectMeta();

	private static class TypeRecord {
		final Class<?>          clazz;
		final Object            mutex  = new Object();
		ObjectMeta              metaObject ;
		KeyValueObject[]        kv         ;
		MethodHandle            filter     ;
		UnaryOperator<Object> transformer;
		@Override public String toString() { return clazz.getCanonicalName(); }
		public TypeRecord(final Class<?> clazz) { this.clazz = clazz; }
	}

	private              boolean                                hasCoreTransformer = false;
	private              int                                    maxRecursiveDepth = 128;
	private              boolean                                skipInvalid = false;
	private              boolean                                failOnUnknownProperties = true;

	private final ConcurrentHashMap<Class<?>, TypeRecord> typeRecordCache = new ConcurrentHashMap<>();

	private TypeRecord getTypeRecord(final Class<?> c) {
		return (typeRecordCache.get(c) instanceof final TypeRecord t  ? t  : typeRecordCache.computeIfAbsent(c, TypeRecord::new));

	}

	public  final ObjectMeta[] metaCache = new ObjectMeta[32768];
	private       int metaCacheSize = 1; // Index 0 ist global reserviert!
	public final ObjectMeta defaultMapMeta;
	public final ObjectMeta genericCollectionMeta;
	public final ObjectMeta genericArrayMeta;
	public final ObjectMeta genericSetMeta;
	final MetaConfig cfg;

	@Override public ObjectMeta[] metaCache() { return metaCache; }

	@Override public void maxRecursiveDepth(final int v) { this.maxRecursiveDepth = v; }
	@Override public int maxRecursiveDepth() { return maxRecursiveDepth; }

	@Override public void skipInvalid(final boolean v) { this.skipInvalid = v; }
	@Override public boolean skipInvalid() { return skipInvalid; }

	@Override public void failOnUnknownProperties(final boolean v) { this.failOnUnknownProperties = v; }
	@Override public boolean failOnUnknownProperties() { return failOnUnknownProperties; }

	final Collection<Predicate<Class<?>>> AUTO_POJO = new CopyOnWriteArrayList<>();
	@Override public void autoPojo(final Predicate<Class<?>> p) { AUTO_POJO.add(p); }

	synchronized int registerMeta(final ObjectMeta meta) {
		if (meta.cacheIndex >= 0) return meta.cacheIndex;
		if (metaCacheSize >= metaCache.length) throw new IllegalStateException("Global MetaCache Limit erreicht");
		final var index = metaCacheSize++;
		metaCache[index] = meta;
		meta.cacheIndex = index;
		return index;
	}

	static long createTypeDesc(final boolean isArray, final boolean isPrimitive, final int cacheIndex) {
		var desc = ((long) cacheIndex) << 1;
		if (isArray)     desc |= 0x8000000000000000L;
		if (isPrimitive) desc |= 1L;
		return desc;
	}

	private final ObjectMeta[] dynamicMetaCache = new ObjectMeta[32];
	private int dynamicMetaCount = 0;

	public synchronized ObjectMeta getDynamicMeta(final Class<?> compType, final int targetMetaType) {
		final var searchType = compType == null ? Object.class : compType;
		if (searchType == Object.class) {
			switch (targetMetaType) {
			case ObjectMeta.TYPE_COLLECTION: return genericCollectionMeta;
			case ObjectMeta.TYPE_OBJ_ARRAY : return genericArrayMeta;
			case ObjectMeta.TYPE_SET       : return genericSetMeta;
			case ObjectMeta.TYPE_MAP       : return defaultMapMeta;
			default                        : break;
			}
		}
		for (var i = 0; i < dynamicMetaCount; i++) {
			final var m = dynamicMetaCache[i];
			if (m.metaType == targetMetaType && m.type(0) == searchType) return m;
		}
		final var m = new ObjectMeta(this, searchType, targetMetaType);
		registerMeta(m);
		m.linkDescriptors(this);
		if (dynamicMetaCount < dynamicMetaCache.length) dynamicMetaCache[dynamicMetaCount++] = m;
		return m;
	}

	// Der Konstruktor:
	public EngineImpl(final MetaConfig cfg) {
		this.cfg = cfg;

		// 1. Erstellen der Metas PRO ENGINE (nicht mehr static!)
		// Je nachdem wie dein ObjectMeta-Konstruktor aussieht, nutze hier Map.class oder Object.class
		this.defaultMapMeta        = new ObjectMeta(this, Map.class);
		this.genericCollectionMeta = new ObjectMeta(this, Object.class, ObjectMeta.TYPE_COLLECTION);
		this.genericArrayMeta      = new ObjectMeta(this, Object.class, ObjectMeta.TYPE_OBJ_ARRAY);
		this.genericSetMeta        = new ObjectMeta(this, Object.class, ObjectMeta.TYPE_SET);

		// 2. Sofortiges Registrieren und Linken in DIESEM engine-spezifischen Cache
		registerMeta(defaultMapMeta);        defaultMapMeta.linkDescriptors(this);
		registerMeta(genericCollectionMeta); genericCollectionMeta.linkDescriptors(this);
		registerMeta(genericArrayMeta);      genericArrayMeta.linkDescriptors(this);
		registerMeta(genericSetMeta);        genericSetMeta.linkDescriptors(this);

		// 3. Mapping für das reguläre Abfragen
		putMeta(Object.class, defaultMapMeta);
		putMeta(java.util.Map.class, defaultMapMeta);
	}

	void putMeta(final Class<?> c, final ObjectMeta m) { getTypeRecord(c).metaObject = m; }

	@Override public JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final Flags... flags)  {
		return JsonOutputStream.of(this, out, timeFormat, flags);
	}

	@Override public JsonOutputStream jsonOutputStream(final OutputStream out, final TimeFormat timeFormat, final int flags)  {
		return JsonOutputStream.of(this, out, timeFormat, flags);
	}

	@Override public JsonOutputStream jsonOutputStream(final OutputStream out) { return JsonOutputStream.of(this, out, null, 0); }

	@Override public UnaryOperator<Object> transformer(final Class<?> c) { return getTypeRecord(c).transformer; }
	@Override public boolean                                hasCoreTransformer() { return hasCoreTransformer; }

	@SuppressWarnings("unchecked")
	@Override public <C> void registerTransformer(final Class<C> c, final UnaryOperator<Object> f) {
		getTypeRecord(c).transformer = f;
		if (c.getClassLoader() == null || c.isArray()) hasCoreTransformer = true;
	}

	@Override public JsonInputStream jsonInputStream(final InputStream is) { return JsonInputStream.of(is, this); }
	@Override public MetaConfig      config         () { return cfg; }
	@Override public MethodHandle    getFilter(final Class<?> c) {
		final var t = getTypeRecord(c);
		var m = t.filter;
		// TODO null sentil with some dummy MH
		if(m == null) m = Meta.newFilter(c);
		return m;
	}

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
		final var t = getTypeRecord(c);
		synchronized (t.mutex) {
			if (t.metaObject instanceof final ObjectMeta r) {
				if (r == ObjectMeta.DEFECT_FIRST || r == DEFECT) throw ObjectMeta.E_DEFEKT2;
				return r == ObjectMeta.NULL ? null : r;
			}
			final ObjectMeta r;
			if     (c.isRecord()) r = ObjectMeta.ofRecord(this, c.asSubclass(Record.class));
			else if(c.isEnum  ()) r = ObjectMeta.ofEnum  (this, c.asSubclass(Enum  .class));
			else                  r = ObjectMeta.ofPojo  (this, c);
			// Java Language Specification (JLS §17.5) - Safe Publication => volatile not required
			/*
		     + --- DIE MAGISCHE BARRIER ---
             + Exakt das, was die JVM am Ende eines final-Konstruktors macht!
             + Zwingt die CPU, die Inhalte von 'temp' in den Speicher zu flushen.
             + > VarHandle.storeStoreFence(); // oder VarHandle.releaseFence();
             + 2. Publikation (Store)
             + Wegen der Fence davor, ist garantiert, dass jeder Thread,
             + der isReady == true sieht, auch zwingend "A" und "B" im Array sieht.
             + > this.isReady = true;
			 */
			t.metaObject = r;
			return r;
		}
	}

	@Override public ObjectMeta metaOf(final Class<?> clazz) {
		if (clazz == null) return null;

		// NUTZE DIE INSTANZ-VARIABLE!
		if (clazz == Object.class || Map.class.isAssignableFrom(clazz)) return this.defaultMapMeta;
		final var t = getTypeRecord(clazz);

		if (t.metaObject instanceof final ObjectMeta r) {
			if (r == ObjectMeta.DEFECT) throw E_DEFEKT1;
			if (r == ObjectMeta.DEFECT_FIRST) { t.metaObject = ObjectMeta.DEFECT; throw new IllegalArgumentException("Clazz: " + clazz.getCanonicalName()); }
			return r == ObjectMeta.NULL ? null : r;
		}
		synchronized (t.mutex) {
			if (t.metaObject instanceof final ObjectMeta r) {
				if (r == ObjectMeta.DEFECT_FIRST || r == DEFECT) throw ObjectMeta.E_DEFEKT2;
				return r == ObjectMeta.NULL ? null : r;
			}
			final var r = createMeta(clazz);
			if (r == ObjectMeta.NULL || r == null) { t.metaObject = ObjectMeta.NULL; return null; }
			if (r == ObjectMeta.DEFECT_FIRST)  { t.metaObject = ObjectMeta.DEFECT; throw new IllegalArgumentException("Clazz: " + clazz.getCanonicalName()); }
			t.metaObject = r;
			registerMeta(r);
			r.linkDescriptors(this);
			return r;
		}
	}

	private static boolean skipClass(final String cls) { return (cls.startsWith("java.") || cls.startsWith("javax.") || cls.startsWith("jdk.") || cls.startsWith("sun.")); }

	@Override
	public KeyValueObject[] ofComplex( final Class<?> c) {
		final var t = getTypeRecord(c);

		if(t.kv instanceof final KeyValueObject[] r) return r == KeyValueObject.NULL ? null : r;
		if (c.getClassLoader() == null || skipClass(c.getName())) { t.kv =KeyValueObject.NULL; return null; }
		synchronized (t.mutex) {
			final var emitClass = cfg.emitClassName();
			KeyValueObject[] rp = null;
			if(Record.class.isAssignableFrom(c))            rp =  KeyValueObject.ofRecord(c.asSubclass(Record.class), cfg);
			else for(final var p : AUTO_POJO) if(p.test(c)) rp =  KeyValueObject.registerComplex(c, cfg);
			if(rp == null) {
				final var once = new HashMap<>();
				for(final var e : typeRecordCache.entrySet()) if(e.getKey().isAssignableFrom(c))
					for(final var fld : e.getValue().kv) once.put(new String(fld.jsonKeyBytes(), StandardCharsets.UTF_8), fld);
				if(once.isEmpty() && !emitClass) { t.kv = KeyValueObject.NULL; return null; }
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
			t.kv = rp;
		}
		final var r = t.kv;
		return r == KeyValueObject.NULL ? null : r;
	}
}