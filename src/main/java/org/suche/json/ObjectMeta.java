package org.suche.json;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.suche.json.ConstructorGenerator.ObjectArrayFactory;
import org.suche.json.MetaPool.ParseContext;

final class ObjectMeta {
	private static final MethodType MT_SUPPLIER = MethodType.methodType(Supplier.class);
	private static final MethodType MT_OBJECT   = MethodType.methodType(Object.class);
	private static final Map<Class<?>, Supplier<Object>> rawsupplier = new ConcurrentHashMap<>();
	private static final long      [] NO_fieldDescriptors = { };
	private static final ObjectMeta[] NO_childMetas       = { };
	private static final Lookup LOOKUP = MethodHandles.lookup();
	private static final ComponentMeta[]      ENUM_COMPS          = { new ComponentMeta(SealedUnionMapper.ENUM_KEY, String.class, Object.class), new ComponentMeta("value", String.class, Object.class) };
	private static final FastKeyTable         ENUM_KEYS           = FastKeyTable.build(ENUM_COMPS);
	private static final int                  MOD_FINAL_OR_STATIC = Modifier.STATIC | Modifier.FINAL;
	private static final Supplier<Object>     NO_POJO_START       = null;
	private static final Class<?>[]           ENUM_TYPES          = { String.class, String.class };
	static         final int PRIM_INT          =  1;
	static         final int PRIM_LONG         =  2;
	static         final int PRIM_DOUBLE       =  3;
	static         final int PRIM_FLOAT        =  4;
	static         final int PRIM_BOOLEAN      =  5;
	static         final int PRIM_BYTE         =  6;
	static         final int PRIM_SHORT        =  7;
	static         final int PRIM_CHAR         =  8;
	static         final int PRIM_STRING       =  9;
	static         final int PRIM_OTHER        = 10;

	static         final int          IDX_GENERIC    = 0;	// Must be 0 for speedup with bit checks
	static         final int          IDX_MAP        = 1;
	static         final int          IDX_COLLECTION = 2;
	static         final int          IDX_OBJ_ARRAY  = 4;
	// Bestehende 64-Bit Deskriptoren für die Engine-Logik:
	static final long DESC_COLLECTION   = (((long) IDX_COLLECTION) << 1) | 0x8000000000000000L;
	static final long DESC_OBJ_ARRAY    = (((long) IDX_OBJ_ARRAY)  << 1) | 0x8000000000000000L;
	static final long DESC_BYTE_ARRAY   = (((long) PRIM_BYTE)      << 1) | 0x8000000000000001L;
	static final long DESC_PRIM_INT     = (((long) PRIM_INT)       << 1) | 1L;
	static final long DESC_PRIM_LONG    = (((long) PRIM_LONG)      << 1) | 1L;
	static final long DESC_PRIM_DOUBLE  = (((long) PRIM_DOUBLE)    << 1) | 1L;
	static final long DESC_PRIM_FLOAT   = (((long) PRIM_FLOAT)     << 1) | 1L;
	static final long DESC_PRIM_BOOLEAN = (((long) PRIM_BOOLEAN)   << 1) | 1L;
	static final long DESC_PRIM_STRING  = (((long) PRIM_STRING)    << 1) | 1L;

	// NEU: Exakte 32-Bit int-Konstanten für den Switch (schneidet Bit 63 zur Compile-Zeit ab):
	static final int SW_PRIM_INT     = (int) DESC_PRIM_INT    ; // 3
	static final int SW_PRIM_LONG    = (int) DESC_PRIM_LONG   ; // 5
	static final int SW_PRIM_DOUBLE  = (int) DESC_PRIM_DOUBLE ; // 7
	static final int SW_PRIM_FLOAT   = (int) DESC_PRIM_FLOAT  ; // 9
	static final int SW_PRIM_BOOLEAN = (int) DESC_PRIM_BOOLEAN; // 11
	static final int SW_BYTE_ARRAY   = (int) DESC_BYTE_ARRAY  ; // 13
	static final int SW_PRIM_STRING  = (int) DESC_PRIM_STRING ;

	static         final int  IDX_CUSTOM_START = 16;
	static         final int  TYPE_INSTANTIATOR = 1;
	static         final int  TYPE_MAP          = 2;
	private static final int  TYPE_DEFECT       = 3;
	static         final int  TYPE_SEALED       = 4;
	static         final int  TYPE_OBJ_ARRAY    = 6;
	static         final int  TYPE_COLLECTION   = 7;

	static         final ObjectMeta           DEFECT_FIRST        = new ObjectMeta(-1);
	static         final ObjectMeta           DEFECT              = new ObjectMeta(-1);
	static         final RuntimeException     E_DEFEKT2           = new RuntimeException("DEFEKT.2", null, false, false) { };
	static         final ObjectMeta           NULL                = new ObjectMeta(null, null, null, -1);
	static         final ObjectMeta           GENERIC_MAP         = new ObjectMeta(null, null, Object.class, IDX_MAP);

	final         int                 metaType;
	final         String              className;
	private final boolean             failOnUnknown;
	final         ObjectArrayFactory  factory;
	private final IntFunction<Object> arrayCreator;
	final         int                 ctorParamCount;
	private final FastKeyTable        keys;
	final         Class<?>[]          types;
	final         Class<?>            baseType;
	final         Class<?>[]          permitted;
	final         Object[][]          enumConstants;
	final         boolean             skipDefaultValues;
	final         boolean             setNumeric0      ;
	final         boolean             setEmpty         ;
	final         boolean             setEmptyString   ;
	final         boolean             needsPrims;
	final         long                componentDescriptor;
	final         int                 cacheIndex;
	private final long[]              fieldDescriptors   ;
	private final ObjectMeta[]        childMetas;
	private final Supplier<Object>    customCollectionStart;
	private final boolean             isComplexComponent;
	// Types for dynamic generic resolution
	final Type genericBaseType;
	final Type genericCompType;

	final ComponentMeta[] components;

	record ComponentMeta(String name, Class<?> type, Type valueType) {
		ComponentMeta {
			if (name == null) throw illegalStateException(null, "Missing name");
			if (type == null) throw illegalStateException(null, "Missing type");
			if (valueType == null) valueType = Object.class;
		}
	}

	private static final class Defect extends RuntimeException implements Supplier<Object> {
		private static final long serialVersionUID = 1L;
		Defect(final String mesg) { super(mesg, null, false, false); }
		@Override public synchronized Throwable fillInStackTrace() { return super.fillInStackTrace(); }
		@Override public void setStackTrace(final StackTraceElement[] stackTrace) { }
		@Override public Object get() { throw this; }
	}

	static record D(Class<?> c, Supplier<Object> s) { static D $(final Class<?> c, final Supplier<Object> s) { return new D(c,s); } }

	private static final D[] COLLECTIONS = {
			D.$(HashSet            .class, HashSet            ::new),	// Set, AbstractSet
			D.$(TreeSet            .class, TreeSet            ::new),	// Set, SortedSet, NavigableSet
			D.$(ArrayDeque         .class, ArrayDeque         ::new),	// Deque, Queue
			D.$(LinkedBlockingDeque.class, LinkedBlockingDeque::new),	// BlockingQueue, AbstractQueue
			D.$(LinkedTransferQueue.class, LinkedTransferQueue::new),	// TransferQueue
			// if(rawBase.isAssignableFrom(ArrayBlockingQueue .class)) return ArrayBlockingQueue ::new;	// BlockingQueue
			// BlockingQueue<E>
			// TransferQueue<E>
			// AbstractSequentialList,
			// ArrayBlockingQueue, ArrayDeque, ArrayList, AttributeList,
			// BeanContextServicesSupport, BeanContextSupport, ConcurrentHashMap.KeySetView,
			// ConcurrentLinkedDeque, ConcurrentLinkedQueue, ConcurrentSkipListSet,
			// CopyOnWriteArrayList, CopyOnWriteArraySet, DelayQueue, EnumSet,
			// JobStateReasons, , LinkedBlockingQueue,
			// LinkedHashSet, LinkedList, , PriorityBlockingQueue,
			// PriorityQueue, RoleList, RoleUnresolvedList, Stack, SynchronousQueue, Vector
	};

	private static final D[] MAPS = {
			D.$(ConcurrentHashMap    .class, ConcurrentHashMap    ::new),	// ConcurrentMap, AbstractMap
			D.$(TreeMap              .class, TreeMap              ::new),	// SortedMap, NavigableMap
			D.$(ConcurrentSkipListMap.class, ConcurrentSkipListMap::new),	// ConcurrentNavigableMap, NavigableMap, SortedMap
	};

	static int getPrimId(final Class<?> type) {
		if (type == int    .class) return PRIM_INT    ;
		if (type == long   .class) return PRIM_LONG   ;
		if (type == double .class) return PRIM_DOUBLE ;
		if (type == float  .class) return PRIM_FLOAT  ;
		if (type == boolean.class) return PRIM_BOOLEAN;
		if (type == byte   .class) return PRIM_BYTE   ;
		if (type == short  .class) return PRIM_SHORT  ;
		if (type == char   .class) return PRIM_CHAR   ;
		if (type == String .class) return PRIM_STRING ;
		return PRIM_OTHER;
	}

	private static boolean isPrimitive(final Class<?> t) {
		if(null == t) return false;
		if(t.isPrimitive() || (String.class == t)) return true;
		return false;
	}

	private static long resolveDescriptor(final InternalEngine e, Type type, final Type valueType) {
		if (type == null) type = Object.class;
		final Class<?> rawType = GernericsHandler.resolveClass(type);
		// If valueType is missing or hardcoded to Object.class, dynamically extract  the generic child type from 'type'.
		// This completely bridges the gap for nested collections and prevents the fallback to CompactMap.
		final var actualValueType = (valueType == null || valueType == Object.class)
				? GernericsHandler.extractValueType(type, rawType) : valueType;

		final var isArray = rawType.isArray() || Collection.class.isAssignableFrom(rawType);
		// final Class<?> resolvedValClass = GernericsHandler.resolveClass(actualValueType);

		// ONLY native Java arrays (e.g. String[], int[]) are primitive arrays.
		// Collections (e.g. HashSet<String>, List<Integer>) MUST resolve their target collection type.
		final var isPrimArray = rawType.isArray() && isPrimitive(rawType.componentType());
		final var isPrimValue = !isArray && isPrimitive(rawType);

		Class<?> primTarget = null;
		if      (isPrimArray) primTarget = rawType.componentType();
		else if (isPrimValue) primTarget = rawType;

		final int subIdx;
		if (primTarget != null) subIdx = getPrimId(primTarget);
		else if (e instanceof final EngineImpl ei) { subIdx = ei.resolveEngineObjectDescriptor(type, actualValueType); }
		else subIdx = IDX_GENERIC;

		return EngineImpl.createTypeDesc(isArray, primTarget != null, subIdx);
	}

	private static Supplier<Object> supplier(final Class<?> rawBase, final Class<?> standard, final D[] defaults) {
		if(rawBase == null || rawBase.isArray()) return null;
		var ret = rawsupplier.get(rawBase);
		if(ret == null) ret = rawsupplier.computeIfAbsent(rawBase, c->builder(c, standard, defaults));
		if(ret instanceof final RuntimeException e) throw e;
		return ret;
	}

	@SuppressWarnings("unchecked")
	private static Supplier<Object> builder(final Class<?> rawBase, final Class<?> standard, final D[] defaults) {
		if(rawBase == null || rawBase.isAssignableFrom(standard)) return null;		// Collection, List, JSONArray, AbstractList, AbstractCollection
		try {
			final var ctor = rawBase.getDeclaredConstructor();
			if (!ctor.canAccess(null)) ctor.setAccessible(true);
			// Einmaliger Testaufruf (Dummy-Objekt) zur Verifizierung vorab
			final var mh = LOOKUP.unreflectConstructor(ctor);
			if(null != mh.invoke()) {
				final var callSite = java.lang.invoke.LambdaMetafactory.metafactory(LOOKUP, "get", MT_SUPPLIER, MT_OBJECT, mh, mh.type());
				return (Supplier<Object>) callSite.getTarget().invokeExact();
			}
		} catch (final Throwable _) {
		}
		if(Modifier.isFinal(rawBase.getModifiers())) return new Defect("final class "+rawBase.getCanonicalName()+" not supported.");
		for(final var e : defaults) if(rawBase.isAssignableFrom(e.c)) return e.s;
		return new Defect("Class "+rawBase.getCanonicalName()+" not supported.");
	}

	long fieldDescriptor(final int idx) {
		return (metaType == TYPE_INSTANTIATOR || metaType == TYPE_SEALED) ? fieldDescriptors[idx] : componentDescriptor;
	}

	ObjectMeta childMeta(final int index) { return childMetas[index]; }

	private static IllegalStateException illegalStateException(final MetaPool location, final String mesg) {
		var m = mesg;
		if(location != null) m = m.replace("{offset}", Long.toString(location.offset()));
		throw new IllegalStateException(m);
	}

	private IllegalStateException invalidKeyException(final String mesg, final String key) {
		throw new IllegalStateException(mesg.replace("{className}", className).replace("{key}",key));
	}

	void invalidType() { throw new IllegalStateException("metaType: "+metaType); }

	static final record Prop(String name, boolean isField, Class<?> jsonType, Class<?> declaredType, Type valueType, int ctorIdx, MethodHandle setterHandle) { }

	private static ObjectMeta[] componentMetaToObjectMeta(final InternalEngine e, final long[] fieldDescriptors) {
		if(fieldDescriptors == null || fieldDescriptors.length == 0) return NO_childMetas;
		final var childMetas = new ObjectMeta[fieldDescriptors.length];
		if(e == null) for (var i = 0; i < fieldDescriptors.length; i++) childMetas[i] = null;
		else for (var i = 0; i < fieldDescriptors.length; i++) {
			final var metaId = (int)(fieldDescriptors[i] >>> 1);
			childMetas[i] = (metaId == ObjectMeta.IDX_GENERIC) ? null : e.metaCache()[metaId];
		}
		return childMetas;
	}

	private static long[]  componentMetaToDescriptor(final InternalEngine e, final ComponentMeta[] pComponents) {
		if(pComponents == null || pComponents.length <= 0) return NO_fieldDescriptors;
		final var descriptors = new long[pComponents.length];
		for (var i = 0; i < pComponents.length; i++) descriptors[i] = resolveDescriptor(e, pComponents[i].type(), pComponents[i].valueType());
		return descriptors;
	}

	// Updated to accept java.lang.reflect.Type and assign generic Types
	ObjectMeta(final InternalEngine e, final Type baseTyp, final Type compType, final int targetMetaType, final int pCacheIndex) {
		this.cacheIndex = pCacheIndex;
		this.genericBaseType = baseTyp;
		this.genericCompType = compType;
		final var rawBase = baseTyp != null ? GernericsHandler.resolveClass(baseTyp) : null;
		final var rawComp = compType != null ? GernericsHandler.resolveClass(compType) : null;
		this.className = targetMetaType == TYPE_OBJ_ARRAY ? "<ARRAY>" : "<COLLECTION>";
		this.metaType = targetMetaType;
		this.failOnUnknown = true;
		this.factory = null;
		this.arrayCreator = (rawComp != null && targetMetaType == TYPE_OBJ_ARRAY) ? size -> Array.newInstance(rawComp, size) : null;
		this.ctorParamCount = 0;
		this.permitted = null;
		this.baseType = rawBase;
		this.keys = new FastKeyTable(new byte[0][], new int[0], 0);
		this.types = new Class<?>[] { rawComp != null ? rawComp : Object.class };
		this.components = new ComponentMeta[0];
		this.enumConstants = null;
		final var cfg = e == null ? null : e.config();
		this.skipDefaultValues = cfg == null ? false : cfg.skipDefaultValues();
		this.setNumeric0       = cfg == null ? false : cfg.setNumeric0();
		this.setEmpty          = cfg == null ? false : cfg.setEmpty   ();
		this.setEmptyString    = cfg == null ? false : cfg.setEmptyString();
		this.needsPrims = rawComp != null && rawComp.isPrimitive();
		this.componentDescriptor = resolveDescriptor(e, compType, Object.class);
		this.fieldDescriptors = componentMetaToDescriptor(e, null                  );
		this.childMetas       = componentMetaToObjectMeta(e, fieldDescriptors);

		// FIX: Überprüfe den echten Meta-Typ des Kind-Objekts!
		final var targetMetaId = (int) (this.componentDescriptor >>> 1);
		if (this.componentDescriptor >= 0L && targetMetaId >= IDX_CUSTOM_START) {
			if(e == null) throw illegalStateException(null, "Missing engine");
			final var childMeta = e.metaCache()[targetMetaId];
			// Nur wenn das Kind-Element ein echtes POJO/Record (TYPE_INSTANTIATOR) oder Sealed-Interface ist,
			// ist es eine komplexe Komponente, die im JSON ein '{' erzwingt!
			this.isComplexComponent = (childMeta != null && (childMeta.metaType == TYPE_INSTANTIATOR || childMeta.metaType == TYPE_SEALED));
		} else {
			this.isComplexComponent = false;
		}
		customCollectionStart = targetMetaType == TYPE_OBJ_ARRAY ? null : supplier(rawBase, CompactList.class, COLLECTIONS);
		if (rawBase != null && !rawBase.isInterface() && !Modifier.isAbstract(rawBase.getModifiers())) {
		}
	}

	private ObjectMeta(final int pCacheIndex) {
		this.cacheIndex = pCacheIndex;
		this.genericBaseType = null;
		this.genericCompType = null;
		this.failOnUnknown = true;
		this.className = null;
		this.metaType = TYPE_DEFECT;
		this.factory = null;
		this.arrayCreator = null;
		this.ctorParamCount = 0;
		this.permitted = null;
		this.keys = null;
		this.types = null;
		this.components = null;
		this.baseType = null;
		this.enumConstants = null;
		this.skipDefaultValues     = false;
		this.setNumeric0           = false;
		this.setEmptyString        = false;
		this.setEmpty              = false;
		this.needsPrims            = false;
		this.customCollectionStart = null ;
		this.isComplexComponent = false;
		this.componentDescriptor   = 0L;
		this.fieldDescriptors      = componentMetaToDescriptor(null, null                  );
		this.childMetas            = componentMetaToObjectMeta(null, fieldDescriptors);
	}

	ObjectMeta(final InternalEngine e, final String pClassName, final Supplier<Object> pStart, final ObjectArrayFactory pFactory, final int pStartCount
			, final FastKeyTable pKeys, final Class<?>[] pTypes, final ComponentMeta[] pComponents, final Object[][] pEnums, final int pCacheIndex) {
		this.cacheIndex            = pCacheIndex;
		this.genericBaseType       = null;
		this.genericCompType       = null;
		this.className             = pClassName;
		this.metaType              = (pStart == null && pFactory == null) ? TYPE_DEFECT : TYPE_INSTANTIATOR;
		this.factory               = pFactory;
		this.arrayCreator          = null;
		this.ctorParamCount        = pStartCount;
		this.permitted             = null;
		this.baseType              = null;
		this.keys                  = pKeys;
		this.types                 = pTypes;
		this.components            = pComponents;
		this.enumConstants         = pEnums;
		this.customCollectionStart = null ;
		this.isComplexComponent    = false;
		this.failOnUnknown = e.failOnUnknownProperties();
		final var cfg = e.config();
		this.skipDefaultValues = cfg == null ? false : cfg.skipDefaultValues();
		this.setNumeric0       = cfg == null ? false : cfg.setNumeric0();
		this.setEmpty          = cfg == null ? false : cfg.setEmpty   ();
		this.setEmptyString    = cfg == null ? false : cfg.setEmptyString();
		var primFound = false;
		for (final var t : pTypes) if (t != null && t.isPrimitive()) { primFound = true; break; }
		this.needsPrims = primFound;
		this.componentDescriptor = 0L;
		this.fieldDescriptors = componentMetaToDescriptor(e, pComponents                  );
		this.childMetas       = componentMetaToObjectMeta(e, fieldDescriptors);
	}

	/** Map constructor */
	ObjectMeta(final InternalEngine e, final Type baseTyp, final Type mapValueType, final int pCacheIndex) {
		final var rawBase = baseTyp      != null ? GernericsHandler.resolveClass(baseTyp     ) : null;
		final var rawVal  = mapValueType != null ? GernericsHandler.resolveClass(mapValueType) : null;
		final var cfg     = e == null ? null : e.config();
		this.cacheIndex            = pCacheIndex;
		this.genericBaseType       = baseTyp;
		this.genericCompType       = mapValueType;
		this.className             = "<MAP>";
		this.metaType              = TYPE_MAP;
		this.failOnUnknown         = false;
		this.factory               = null;
		this.arrayCreator          = null;
		this.ctorParamCount        = 0;
		this.permitted             = null;
		this.baseType              = rawBase;
		this.keys                  = new FastKeyTable(new byte[0][], new int[0], 0);
		this.types                 = new Class<?>[] { rawVal };
		this.components            = new ComponentMeta[0];
		this.enumConstants         = null;
		this.skipDefaultValues     = cfg == null ? false : cfg.skipDefaultValues();
		this.setNumeric0           = cfg == null ? false : cfg.setNumeric0();
		this.setEmpty              = cfg == null ? false : cfg.setEmpty   ();
		this.setEmptyString        = cfg == null ? false : cfg.setEmptyString();
		this.needsPrims            = rawVal != null && rawVal.isPrimitive();
		this.componentDescriptor   = resolveDescriptor(e, mapValueType, Object.class);
		this.fieldDescriptors      = new long[] { this.componentDescriptor };
		this.childMetas            = componentMetaToObjectMeta(e, fieldDescriptors);
		this.isComplexComponent    = false;
		// Pre-compile instance creator for custom maps
		this.customCollectionStart = supplier(rawBase, CompactMap.class, MAPS);
	}

	Class<?> type(final int index) { return types[1==types.length?0:index]; }

	long getChildDescriptor(final int index) {
		return (this.metaType == TYPE_INSTANTIATOR || this.metaType == TYPE_SEALED) ? fieldDescriptors[index] : this.componentDescriptor;
	}

	// The unified main constructor for TYPE_SEALED
	ObjectMeta(final InternalEngine e, final Type baseTyp, final String pClassName, final Class<?>[] pSubclasses, final String[] pKeys, final Class<?>[] pTypes, final ComponentMeta[] pComponents, final int pCacheIndex) {
		if(pTypes == null) throw illegalStateException(null, "Missing pTyps");
		this.cacheIndex     = pCacheIndex;
		this.genericBaseType = baseTyp;
		this.genericCompType = null;
		this.className      = pClassName;
		final var possibleComponents = (pComponents != null) ? pComponents : new ComponentMeta[pKeys.length];
		if (pComponents == null) {
			for (var i = 0; i < pKeys.length; i++) possibleComponents[i] = new ComponentMeta(pKeys[i], pTypes[i], Object.class);
		}
		this.failOnUnknown         = e != null && e.failOnUnknownProperties();
		this.metaType              = TYPE_SEALED;
		this.factory               = null;
		this.arrayCreator          = null;
		this.ctorParamCount        = 0;
		this.isComplexComponent    = false;
		this.permitted             = pSubclasses;
		this.baseType              = baseTyp != null ? GernericsHandler.resolveClass(baseTyp) : null;
		this.keys                  = FastKeyTable.build(possibleComponents);
		this.types                 = pTypes;
		this.customCollectionStart = null ;
		this.components     = possibleComponents;
		this.enumConstants  = buildEnumConstants(pTypes);
		final var cfg = e == null ? null : e.config();
		this.skipDefaultValues = cfg == null ? false : cfg.skipDefaultValues();
		this.setNumeric0       = cfg == null ? false : cfg.setNumeric0();
		this.setEmpty          = cfg == null ? false : cfg.setEmpty   ();
		this.setEmptyString    = cfg == null ? false : cfg.setEmptyString();
		var primFound = false;
		for (final var t : pTypes) if (t != null && t.isPrimitive()) { primFound = true; break; }
		this.needsPrims          = primFound;
		this.componentDescriptor = 0L;
		this.fieldDescriptors    = componentMetaToDescriptor(e, possibleComponents);
		this.childMetas          = componentMetaToObjectMeta(e, fieldDescriptors);
	}

	long getComponentDescriptor() { return componentDescriptor; }

	static ObjectMeta ofRecord(final InternalEngine engine, final Type genericType, final Class<? extends Record> c, final int cacheIndex) {
		final var comps = c.getRecordComponents();
		final var metaComps = new ComponentMeta[comps.length];
		final var constructorTypes = new Class<?>[comps.length];
		final var jsonTypes        = new Class<?>[comps.length];
		try {
			for (var i = 0; i < comps.length; i++) {
				final var comp = comps[i];
				var name = comp.getName();
				if(comp.getAnnotation(org.suche.json.JsonProperty.class) instanceof final org.suche.json.JsonProperty p && !p.value().isEmpty()) name = p.value();
				// Resolved type for JSON logic
				final var actualFieldType = GernericsHandler.resolveActualType(comp.getGenericType(), genericType);
				jsonTypes[i] = GernericsHandler.resolveClass(actualFieldType);
				// 1. Erased type for Reflection (Constructor lookup)
				constructorTypes[i] = comp.getType();
				// The resolved type (e.g., Station.class) goes EXCLUSIVELY into the valueType...
				metaComps[i] = new ComponentMeta(name, jsonTypes[i], GernericsHandler.extractValueType(comp.getGenericType(), genericType));
				// System.out.println("metaComps[i] "+metaComps[i]+" CT: "+constructorTypes[i]);	// type=List value=XY ctor=Object
			}
			final var factory = ConstructorGenerator.generate(c, constructorTypes);
			return new ObjectMeta(engine, genericType.getTypeName(), NO_POJO_START, factory, comps.length, FastKeyTable.build(metaComps), jsonTypes, metaComps, buildEnumConstants(jsonTypes), cacheIndex);
		} catch (final Exception e) {
			e.printStackTrace();
			return DEFECT_FIRST;
		}
	}

	static ObjectMeta ofPojo(final InternalEngine engine, final Type genericType, final Class<?> c, final int cacheIndex) {
		if(c.getCanonicalName().startsWith("java.lang.String")) throw illegalStateException(null, c.getCanonicalName());
		try {
			final var ctors = c.getDeclaredConstructors();
			Constructor<?> bestCtor = null;
			for (final var ctor : ctors) {
				if (ctor.getParameterCount() == 0) { bestCtor = ctor; break; }
				if (bestCtor == null || ctor.getParameterCount() > bestCtor.getParameterCount()) bestCtor = ctor;
			}
			if (bestCtor == null) return DEFECT_FIRST;
			if (!bestCtor.canAccess(null)) bestCtor.setAccessible(true);
			final var params       = bestCtor.getParameters();
			final var props        = pojoProps(genericType, c, params);
			final var totalProps   = props.size();
			final var finalComps   = new ComponentMeta[totalProps];
			final var finalTypes   = new Class<?>[totalProps];
			final var ctorArgs     = params.length;
			final var propDefsList = new java.util.ArrayList<ConstructorGenerator.PropDef>();
			final var ctorTypes    = new Class<?>[ctorArgs];

			var setterCounter = ctorArgs;
			for (final var entry : props.entrySet()) {
				final var  jsonKey = entry.getKey();
				final var  prop    = entry.getValue();
				final var  targetIdx = prop.ctorIdx != -1 ? prop.ctorIdx : setterCounter++;

				// Verwende jsonType für die JSON Metadaten
				finalComps[targetIdx] = new ComponentMeta(jsonKey, prop.jsonType(), prop.valueType());
				finalTypes[targetIdx] = prop.jsonType();

				// Verwende declaredType (erased) für die Reflection Generator Aufrufe
				if (prop.ctorIdx != -1) ctorTypes[prop.ctorIdx] = prop.declaredType();
				else propDefsList.add(new ConstructorGenerator.PropDef(prop.name(), prop.declaredType(), prop.isField()));
			}

			final var keys          = FastKeyTable.build(finalComps);
			final var enumConstants = buildEnumConstants(finalTypes);
			final var propDefs      = propDefsList.isEmpty() ? null : propDefsList.toArray(new ConstructorGenerator.PropDef[0]);
			final var factory       = ConstructorGenerator.generate(c, "<init>", ctorTypes, propDefs);

			return new ObjectMeta(engine, genericType.getTypeName(), NO_POJO_START, factory, ctorArgs, keys, finalTypes, finalComps, enumConstants, cacheIndex);
		} catch (final Exception e) {
			e.printStackTrace();
			return DEFECT_FIRST;
		}
	}

	private static LinkedHashMap<String, Prop> pojoProps(final Type genericContext, final Class<?> c, final Parameter[] params) {
		if(c.getCanonicalName().startsWith("java.lang.")) throw illegalStateException(null, c.getCanonicalName());
		final var props = new LinkedHashMap<String, Prop>();

		for (var i = 0; i < params.length; i++) {
			final var paramGenType = params[i].getParameterizedType();
			final var actualType = GernericsHandler.resolveActualType(paramGenType, genericContext);
			final var resolvedClass = GernericsHandler.resolveClass(actualType);
			final var resolvedVal = GernericsHandler.extractValueType(paramGenType, genericContext);
			props.put(params[i].getName(), new Prop(params[i].getName(), false, resolvedClass, params[i].getType(), resolvedVal, i, null));
		}

		for (final var e : c.getMethods()) {
			if (e.getParameterCount() != 1 || e.getName().length() < 4 || !e.getName().startsWith("set")) continue;
			final var javaName = e.getName();
			var jsonName = Character.toLowerCase(javaName.charAt(3)) + javaName.substring(4);
			if(e.getAnnotation(org.suche.json.JsonProperty.class) instanceof final org.suche.json.JsonProperty p && !p.value().isEmpty()) {
				jsonName = p.value();
			}
			if (!props.containsKey(jsonName)) {
				e.setAccessible(true);
				final var paramGenType = e.getGenericParameterTypes()[0];
				final var actualType = GernericsHandler.resolveActualType(paramGenType, genericContext);
				final var resolvedClass = GernericsHandler.resolveClass(actualType);
				final var resolvedVal = GernericsHandler.extractValueType(paramGenType, genericContext);
				props.put(jsonName, new Prop(javaName, false, resolvedClass, e.getParameterTypes()[0], resolvedVal, -1, null));
			}
		}

		for (final var e : c.getDeclaredFields()) {
			if (0 != (e.getModifiers() & MOD_FINAL_OR_STATIC)) continue;
			final var javaName = e.getName();
			var jsonName = javaName;
			if(e.getAnnotation(org.suche.json.JsonProperty.class) instanceof final org.suche.json.JsonProperty p && !p.value().isEmpty()) {
				jsonName = p.value();
			}
			if (!props.containsKey(jsonName)) {
				try { e.setAccessible(true); } catch(final Throwable _) { }
				final var fieldGenType = e.getGenericType();
				final var actualType    = GernericsHandler.resolveActualType(fieldGenType, genericContext);
				final var resolvedClass = GernericsHandler.resolveClass     (actualType);
				final var resolvedVal   = GernericsHandler.extractValueType (fieldGenType, genericContext);
				props.put(jsonName, new Prop(javaName, true, resolvedClass, e.getType(), resolvedVal, -1, null));
			}
		}
		return props;
	}

	static ObjectMeta ofSealed(final InternalEngine engine, final Class<?> c, final int cacheIndex) {
		try {
			final var permitted = c.getPermittedSubclasses();
			if (permitted == null || permitted.length == 0) return DEFECT_FIRST;

			final var propMap = new LinkedHashMap<String, ComponentMeta>();
			propMap.put(SealedUnionMapper.ENUM_KEY, new ComponentMeta(SealedUnionMapper.ENUM_KEY, String.class, Object.class));
			propMap.put("type", new ComponentMeta("type", String.class, Object.class));
			propMap.put("__type__", new ComponentMeta("__type__", String.class, Object.class));
			propMap.put("@type", new ComponentMeta("@type", String.class, Object.class));

			for (final Class<?> sub : permitted) {
				final var subMeta = engine.metaOf(sub);
				if (subMeta != null && subMeta.components != null) {
					for (final var comp : subMeta.components) {
						propMap.putIfAbsent(comp.name(), comp);
					}
				}
			}

			final var comps = propMap.values().toArray(new ComponentMeta[0]);
			final var keys  = new String[comps.length];
			final var types = new Class<?>[comps.length];
			for (var i = 0; i < comps.length; i++) {
				keys[i]  = comps[i].name();
				types[i] = comps[i].type();
			}

			return new ObjectMeta(engine, c, c.getCanonicalName(), permitted, keys, types, comps, cacheIndex);
		} catch (final Exception e) {
			e.printStackTrace();
			return DEFECT_FIRST;
		}
	}

	static ObjectMeta ofEnum(final InternalEngine e, final Class<?> c, final int cacheIndex) {
		final var values = c.getEnumConstants();
		return new ObjectMeta(e, c.getCanonicalName(), NO_POJO_START, (objects, _) -> Meta.resolveEnum(values, objects[1]), 0, ENUM_KEYS, ENUM_TYPES, ENUM_COMPS, null, cacheIndex);
	}

	private static Object[][] buildEnumConstants(final Class<?>[] types) {
		if (types == null) return null;
		var hasEnums = false;
		final var enums = new Object[types.length][];
		for (var i = 0; i < types.length; i++) {
			if (types[i] != null && types[i].isEnum()) {
				enums[i] = types[i].getEnumConstants();
				hasEnums = true;
			}
		}
		return hasEnums ? enums : null;
	}

	ComponentMeta[] components() { return components; }
	public Class<?> typeOf(final int index) { return (components==null||index<0||index>=components.length ? Object.class : this.components[index].type()); }

	int prepareKey(final int hash, final byte[] buffer, final int off, final int len) {
		if (metaType == TYPE_MAP) return -1;
		final var idx = keys.get(hash, buffer, off, len);
		ArrayList<String> unknownKeys = null;
		if(idx == -1 && failOnUnknown) {
			if(unknownKeys == null) unknownKeys = new ArrayList<>();
			unknownKeys.add(new String(buffer, off, len));
		}
		if(unknownKeys != null) throw invalidKeyException("JSON: Unknown property.0 [{key}] in class {className}", unknownKeys.toString());
		return idx;
	}

	int prepareKey(final Object context, final String key) {
		if (metaType == TYPE_MAP) {
			if(null == context) throw JsonEngine.illegalStateException("Context must not be null");
			((ParseContext) context).currentKey = key;
			return 0;
		}
		final var b = key.getBytes(StandardCharsets.UTF_8);
		final var hash = BufferedStream.computeHash(b, 0, b.length);
		final var idx = keys.get(hash, b, 0, b.length);
		if (idx == -1 && failOnUnknown) throw invalidKeyException("JSON: Unknown property.1 {key} in class {className}", key);
		return idx;
	}

	Object start(final MetaPool s) {
		return switch (metaType) {
		case TYPE_MAP, TYPE_OBJ_ARRAY, TYPE_COLLECTION -> s.takeContext(TYPE_MAP == metaType);
		case TYPE_INSTANTIATOR, TYPE_SEALED -> {
			final var len = fieldDescriptors != null ? fieldDescriptors.length : 16;
			final var ctx = s.takeContext(false);
			if (ctx.objs == null) ctx.objs = s.takeArray(len);
			if (needsPrims && ctx.prims == null) ctx.prims = s.takeLongArray(len);
			ctx.cnt = len;
			yield ctx;
		}
		default -> illegalStateException(s, Integer.toString(metaType));
		};
	}

	private Object endCollection(final MetaPool s, final Object context) {
		final var ctx = (ParseContext) context;
		final var cnt = ctx.cnt;
		if (setEmpty && cnt == 0) {
			s.returnContext(ctx);
			if (customCollectionStart != null) return customCollectionStart.get();
			return EmptyJSONArray.ONCE;
		}
		if (customCollectionStart != null) {
			@SuppressWarnings("unchecked")
			final var c = (Collection<Object>) customCollectionStart.get();
			if (c != null) {
				if (ctx.objs != null) {
					for (var i = 0; i < cnt; i++) c.add(ctx.objs[i]);
				}
				s.returnContext(ctx);
				return c;
			}
		}
		if (cnt == 0) {
			s.returnContext(ctx);
			return EmptyJSONArray.ONCE;
		}

		final var primsLen   = ctx.prims == null ? 0 : ctx.prims.length;
		final var objsLen    = ctx.objs  == null ? 0 : ctx.objs .length;
		final var wastePrims = primsLen - cnt;
		final var wasteObjs  = objsLen  - cnt;

		long[] p;
		Object[] o;

		if (ctx.prims != null && wastePrims <= 16) {
			p = ctx.prims;
			ctx.prims = null;
		} else {
			p = ctx.prims == null ? null : Arrays.copyOf(ctx.prims, cnt);
		}

		if (ctx.singleType == PRIMITIVE.T_LONG || ctx.singleType == PRIMITIVE.T_DOUBLE) {
			o = null;
		} else if (ctx.objs != null && wasteObjs <= 16) {
			o = ctx.objs;
			ctx.objs = null;
		} else {
			o = ctx.objs == null ? null : Arrays.copyOf(ctx.objs, cnt);
		}

		final var ret = new CompactList(ctx.singleType, o, p, cnt);
		s.returnContext(ctx);
		return ret;
	}

	@SuppressWarnings("unchecked")
	Object end(final MetaPool s, final Object context) throws Throwable {
		return switch (metaType) {
		case TYPE_INSTANTIATOR -> {
			final var ctx    = (ParseContext) context;
			final var result = factory.create(ctx.objs, ctx.prims);
			s.returnContext(ctx);
			yield result;
		}
		case TYPE_MAP -> {
			final var ctx = (ParseContext) context;
			if (setEmpty && ctx.cnt == 0) {
				s.returnContext(ctx);
				if (customCollectionStart != null) yield customCollectionStart.get();
				yield new CompactMap(PRIMITIVE.T_EMPTY, null, null);
			}
			if (ctx.objs == null) yield null;
			if (customCollectionStart != null) {
				final var map = (Map<Object, Object>) customCollectionStart.get();
				if (map != null) {
					for (var i = 0; i < ctx.cnt; i += 2) {
						final var key = ctx.objs[i  ];
						final var val = ctx.objs[i+1];
						if (key != null && val != null) map.put(key, val);
					}
					s.returnContext(ctx);
					yield map;
				}
			}

			final var objsLen    = ctx.objs.length;
			final var primsLen   = ctx.prims == null ? 0 : ctx.prims.length;
			final var wasteObjs  = objsLen - ctx.cnt;
			final var wastePrims = primsLen - (ctx.cnt >> 1);

			Object[] data;
			long[] prims;

			if (wasteObjs <= 16) {
				data = ctx.objs;
				ctx.objs = null;
			} else {
				data = Arrays.copyOf(ctx.objs, ctx.cnt);
			}

			if (ctx.prims == null) {
				prims = null;
			} else if (wastePrims <= 16) {
				prims = ctx.prims;
				ctx.prims = null;
			} else {
				prims = Arrays.copyOf(ctx.prims, ctx.cnt >> 1);
			}

			s.returnContext(ctx);
			yield new CompactMap(ctx.singleType, data, prims);
		}
		case TYPE_OBJ_ARRAY -> {
			final var ctx = (ParseContext) context;
			if (setEmpty && ctx.cnt == 0) { s.returnContext(ctx); yield null; }
			final var result = arrayCreator.apply(ctx.cnt);
			if (ctx.objs != null) System.arraycopy(ctx.objs, 0, result, 0, ctx.cnt);
			else if (ctx.prims != null) {
				// Box primitive values into target array (e.g., for Double[] vs double[])
				if (ctx.singleType == PRIMITIVE.T_DOUBLE) for (var i = 0; i < ctx.cnt; i++) Array.set(result, i, Double.longBitsToDouble(ctx.prims[i]));
				else if (ctx.singleType == PRIMITIVE.T_LONG) for (var i = 0; i < ctx.cnt; i++) Array.set(result, i, ctx.prims[i]);
			}
			s.returnContext(ctx);
			yield result;
		}
		case TYPE_COLLECTION -> endCollection(s, context);
		case TYPE_SEALED     -> SealedUnionMapper.end(s, context, baseType, permitted, keys, types);
		default              -> throw illegalStateException(s, Integer.toString(metaType));
		};
	}

	private void checkComplexTypeConstraint(final MetaPool location) {
		if (isComplexComponent) {
			throw illegalStateException(location, "At offset {offset} type mismatch: Expected JSON Object for complex type, but got a primitive value");
		}
	}

	private void checkComplexTypeConstraint(final MetaPool location, final Object value) {
		if (isComplexComponent && (value instanceof String || value instanceof Number || value instanceof Boolean)) {
			throw illegalStateException(location, "At offset {offset} type mismatch: Expected JSON Object for complex type, but got a primitive value or String");
		}
	}

	// @SuppressWarnings("unchecked")
	void setLong(final MetaPool s, final Object context, final int index, final long v) {
		if (setNumeric0 && v == 0L) return;
		switch (metaType) {
		case TYPE_INSTANTIATOR, TYPE_SEALED  -> ((ParseContext)context).prims[index] = v;
		case TYPE_MAP                        -> ((ParseContext)context).primKeyValue(s, PRIMITIVE.LONG, v);
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> { checkComplexTypeConstraint(s); ((ParseContext)context).primIdxValue(s, PRIMITIVE.LONG, v, index); }
		default -> set(s, context, index, v);
		}
	}

	// @SuppressWarnings("unchecked")
	void setDouble(final MetaPool s, final Object context, final int index, final double v) {
		if (setNumeric0 && v == 0.0) return;
		final var bits = Double.doubleToRawLongBits(v);
		switch (metaType) {
		case TYPE_INSTANTIATOR, TYPE_SEALED  -> ((ParseContext)context).prims[index] = bits;
		case TYPE_MAP                        -> ((ParseContext)context).primKeyValue(s, PRIMITIVE.DOUBLE, bits);
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> { checkComplexTypeConstraint(s); ((ParseContext)context).primIdxValue(s, PRIMITIVE.DOUBLE, bits, index); }
		default                              -> set(s, context, index, v);
		}
	}

	// @SuppressWarnings("unchecked")
	void set(final MetaPool s, final Object context, final int index, Object value) {
		switch (metaType) {
		case TYPE_INSTANTIATOR, TYPE_SEALED -> {
			final var pc = (ParseContext)context;
			if (value == null) { pc.objs [index] = null; pc.prims[index] = 0; return; }
			if (this.enumConstants != null && this.enumConstants[index] != null) value = Meta.resolveEnum(this.enumConstants[index], value);
			pc.objs[index] = value;
		}
		case TYPE_MAP -> {
			if (value == null && setEmpty) return;
			if (value instanceof final String t && t.isEmpty() && !setEmptyString) return;
			final var ctx = (ParseContext) context;
			ctx.upgradeToMixed(s, ctx.cnt + 2);
			ctx.objs[ctx.cnt++] = ctx.currentKey;
			ctx.objs[ctx.cnt++] = value;
			ctx.currentKey = null;
		}
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> {
			if (value == null && setEmpty && metaType != TYPE_OBJ_ARRAY && metaType != TYPE_COLLECTION) return;
			if (value instanceof final String t && t.isEmpty() && !setEmptyString) return;
			checkComplexTypeConstraint(s, value);
			final var ctx = (ParseContext) context;
			ctx.upgradeToMixed(s, index + 1);
			if (ctx.objs == null || index >= ctx.objs.length) ctx.ensureObjs(s, index + 1);
			ctx.objs[index] = value;
			if (index >= ctx.cnt) ctx.cnt = index + 1;
		}
		default -> invalidType();
		}
	}
}