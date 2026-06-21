package org.suche.json;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.suche.json.ConstructorGenerator.ObjectArrayFactory;
import org.suche.json.MetaPool.ParseContext;

final class ObjectMeta {
	static         final int          IDX_GENERIC    = 0;	// Must be 0 for speedup with bit checks
	static         final int          IDX_MAP        = 1;
	static         final int          IDX_COLLECTION = 2;
	static         final int          IDX_SET        = 3;
	static         final int          IDX_OBJ_ARRAY  = 4;
	private static final long      [] NO_fieldDescriptors = { };
	private static final ObjectMeta[] NO_childMetas       = { };

	static         final long DESC_COLLECTION = EngineImpl.createTypeDesc(true, false, IDX_COLLECTION);
	static         final long DESC_OBJ_ARRAY  = EngineImpl.createTypeDesc(true, false, IDX_OBJ_ARRAY);

	static         final int IDX_CUSTOM_START = 16;

	static         final int TYPE_INSTANTIATOR = 1;
	static         final int TYPE_MAP          = 2;
	private static final int TYPE_DEFECT       = 3;
	static         final int TYPE_SEALED       = 4;
	static         final int TYPE_OBJ_ARRAY    = 6;
	static         final int TYPE_COLLECTION   = 7;
	static         final int TYPE_SET          = 8;

	static         final int PRIM_INT          = 1;
	static         final int PRIM_LONG         = 2;
	static         final int PRIM_DOUBLE       = 3;
	static         final int PRIM_FLOAT        = 4;
	static         final int PRIM_BOOLEAN      = 5;
	static         final int PRIM_OTHER        = 6;

	private static final ComponentMeta[]      ENUM_COMPS          = { new ComponentMeta(SealedUnionMapper.ENUM_KEY, String.class, Object.class), new ComponentMeta("value", String.class, Object.class) };
	private static final FastKeyTable         ENUM_KEYS           = FastKeyTable.build(ENUM_COMPS);
	private static final int                  MOD_FINAL_OR_STATIC = Modifier.STATIC | Modifier.FINAL;
	private static final ObjectArrayFactory   NO_FACTORY          = null;
	private static final int                  NO_FACTORY_PARAM    = 0;
	private static final Supplier<Object>     NO_POJO_START       = null;
	private static final MethodHandles.Lookup LOOKUP              = MethodHandles.lookup();
	private static final Class<?>[]           ENUM_TYPES          = { String.class, String.class };
	static         final ObjectMeta           DEFECT_FIRST        = new ObjectMeta(-1);
	static         final ObjectMeta           DEFECT              = new ObjectMeta(-1);
	static         final RuntimeException     E_DEFEKT2           = new RuntimeException("DEFEKT.2", null, false, false) { };
	static         final ObjectMeta           NULL                = new ObjectMeta(null, null, null, -1);
	static         final ObjectMeta           GENERIC_MAP         = new ObjectMeta(null, null, Object.class, IDX_MAP);

	static int getPrimId(final Class<?> type) {
		if (type == int.class    ) return PRIM_INT;
		if (type == long.class   ) return PRIM_LONG;
		if (type == double.class ) return PRIM_DOUBLE;
		if (type == float.class  ) return PRIM_FLOAT;
		if (type == boolean.class) return PRIM_BOOLEAN;
		return PRIM_OTHER;
	}

	final         int                 metaType;
	private final String              className;
	private final boolean             failOnUnknown;
	final         Supplier<Object>    pojoStart;
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

	private final int[]               lastSeenSizeByDepth = new int[64];
	final         boolean             needsPrims;

	final         long                componentDescriptor;

	final         int                 cacheIndex;
	private final long[]              fieldDescriptors   ;
	private final ObjectMeta[]        childMetas;

	long fieldDescriptor(final int idx) {
		// POJOs use field arrays, all others (Map, List, Set) use componentDescriptor!
		return metaType == TYPE_INSTANTIATOR ? fieldDescriptors[idx] : componentDescriptor;
	}

	ObjectMeta childMeta(final int index) { return childMetas[index]; }

	private void lastSize(final int depth, final int size) { if (depth >= 0 && depth < 64) lastSeenSizeByDepth[depth] = size; }

	record ComponentMeta(String name, Class<?> type, Class<?> valueType) {
		ComponentMeta {
			if (name == null) throw new IllegalStateException("Missing name");
			if (type == null) throw new IllegalStateException("Missing type");
			if (valueType == null) valueType = Object.class;
		}
	}

	final ComponentMeta[] components;

	static final record Prop(String name, boolean isField, Class<?> type, Class<?> valueType, int ctorIdx, MethodHandle setterHandle) { }

	private static long resolveDescriptor(final InternalEngine e, Class<?> type, Class<?> valueType) {
		if (type      == null) type = Object.class;
		if (valueType == null) valueType = Object.class;
		final var isArray = type.isArray() || Collection.class.isAssignableFrom(type);
		final var isPrimArray = isArray && (type.isArray() ? type.componentType().isPrimitive() : valueType.isPrimitive());
		final var isPrimValue = !isArray && type.isPrimitive();
		Class<?> primTarget = null;
		if      (isPrimArray) primTarget = type.isArray() ? type.componentType() : valueType;
		else if (isPrimValue) primTarget = type;
		final int subIdx;
		if (primTarget != null) subIdx = getPrimId(primTarget);
		else if (e instanceof final EngineImpl ei) subIdx = ei.resolveEngineObjectDescriptor(type, valueType);
		else subIdx = IDX_GENERIC;
		return EngineImpl.createTypeDesc(isArray, primTarget != null, subIdx);
	}

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

	ObjectMeta(final InternalEngine e, final Class<?> baseTyp, final Class<?> compType, final int targetMetaType, final int pCacheIndex) {
		this.cacheIndex = pCacheIndex;
		this.className = targetMetaType == TYPE_OBJ_ARRAY ? "<ARRAY>" : (targetMetaType == TYPE_SET ? "<SET>" : "<COLLECTION>");
		this.metaType = targetMetaType;
		this.failOnUnknown = true;
		this.pojoStart = null;
		this.factory = null;
		this.arrayCreator = (compType != null && targetMetaType == TYPE_OBJ_ARRAY) ? size -> Array.newInstance(compType, size) : null;
		this.ctorParamCount = 0;
		this.permitted = null;
		this.baseType = baseTyp;
		this.keys = new FastKeyTable(new byte[0][], new int[0], 0);
		this.types = new Class<?>[] { compType != null ? compType : Object.class };
		this.components = new ComponentMeta[0];
		this.enumConstants = null;
		final var cfg = e.config();
		this.skipDefaultValues = cfg == null ? false : cfg.skipDefaultValues();
		this.setNumeric0       = cfg == null ? false : cfg.setNumeric0();
		this.setEmpty          = cfg == null ? false : cfg.setEmpty   ();
		this.needsPrims = compType != null && compType.isPrimitive();
		this.componentDescriptor = resolveDescriptor(e, compType, Object.class);
		this.fieldDescriptors = componentMetaToDescriptor(e, null                  );
		this.childMetas       = componentMetaToObjectMeta(e, fieldDescriptors);
	}

	private ObjectMeta(final int pCacheIndex) {
		this.cacheIndex = pCacheIndex;
		this.pojoStart = null;
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
		this.skipDefaultValues = false;
		this.setNumeric0       = false;
		this.setEmpty          = false;
		this.needsPrims = false;
		this.componentDescriptor = 0L;
		this.fieldDescriptors = componentMetaToDescriptor(null, null                  );
		this.childMetas       = componentMetaToObjectMeta(null, fieldDescriptors);
	}

	ObjectMeta(final InternalEngine e, final String pClassName, final Supplier<Object> pStart, final ObjectArrayFactory pFactory, final int pStartCount
			, final FastKeyTable pKeys, final Class<?>[] pTypes, final ComponentMeta[] pComponents, final Object[][] pEnums, final int pCacheIndex) {
		this.cacheIndex = pCacheIndex;
		this.className = pClassName;
		this.pojoStart = pStart;
		this.metaType = (pStart == null && pFactory == null) ? TYPE_DEFECT : TYPE_INSTANTIATOR;
		this.factory = pFactory;
		this.arrayCreator = null;
		this.ctorParamCount = pStartCount;
		this.permitted = null;
		this.baseType = null;
		this.keys = pKeys;
		this.types = pTypes;
		this.components = pComponents;
		this.enumConstants = pEnums;
		this.failOnUnknown = e.failOnUnknownProperties();
		final var cfg = e.config();
		this.skipDefaultValues = cfg == null ? false : cfg.skipDefaultValues();
		this.setNumeric0       = cfg == null ? false : cfg.setNumeric0();
		this.setEmpty          = cfg == null ? false : cfg.setEmpty   ();
		var primFound = false;
		for (final var t : pTypes) if (t != null && t.isPrimitive()) { primFound = true; break; }
		this.needsPrims = primFound;
		this.componentDescriptor = 0L;
		this.fieldDescriptors = componentMetaToDescriptor(e, pComponents                  );
		this.childMetas       = componentMetaToObjectMeta(e, fieldDescriptors);
	}

	ObjectMeta(final InternalEngine e, final Class<?> baseTyp, final Class<?> mapValueType, final int pCacheIndex) {
		this.cacheIndex     = pCacheIndex;
		this.className      = "<MAP>";
		this.metaType       = TYPE_MAP;
		this.failOnUnknown  = false;
		this.pojoStart      = null;
		this.factory        = null;
		this.arrayCreator   = null;
		this.ctorParamCount = 0;
		this.permitted      = null;
		this.baseType       = baseTyp;
		this.keys           = new FastKeyTable(new byte[0][], new int[0], 0);
		this.types          = new Class<?>[] { mapValueType };
		this.components     = new ComponentMeta[0];
		this.enumConstants  = null;
		final var cfg = e == null ? null : e.config();
		this.skipDefaultValues = cfg == null ? false : cfg.skipDefaultValues();
		this.setNumeric0       = cfg == null ? false : cfg.setNumeric0();
		this.setEmpty          = cfg == null ? false : cfg.setEmpty   ();
		this.needsPrims = mapValueType != null && mapValueType.isPrimitive();
		this.componentDescriptor = resolveDescriptor(e, mapValueType, Object.class);
		this.fieldDescriptors = new long[] { this.componentDescriptor };
		this.childMetas       = componentMetaToObjectMeta(e, fieldDescriptors);
	}

	ObjectMeta(final InternalEngine e,final String pClassName, final Class<?>[] pSubclasses, final String[] pKeys, final Class<?>[] pTypes, final int pCacheIndex) {
		this.cacheIndex     = pCacheIndex;
		this.className      = pClassName;
		final var possibleComponents = new ComponentMeta[pKeys.length];
		this.failOnUnknown  = e.failOnUnknownProperties();
		for (var i = 0; i < pKeys.length; i++) possibleComponents[i] = new ComponentMeta(pKeys[i], pTypes[i], Object.class);
		this.metaType       = TYPE_SEALED;
		this.pojoStart      = null;
		this.factory        = null;
		this.arrayCreator   = null;
		this.ctorParamCount = 0;
		this.permitted      = pSubclasses;
		this.baseType       = null;
		this.keys           = FastKeyTable.build(possibleComponents);
		this.types          = pTypes;
		this.components     = possibleComponents;
		this.enumConstants  = buildEnumConstants(pTypes);
		final var cfg = e.config();
		this.skipDefaultValues = cfg == null ? false : cfg.skipDefaultValues();
		this.setNumeric0       = cfg == null ? false : cfg.setNumeric0();
		this.setEmpty          = cfg == null ? false : cfg.setEmpty   ();
		this.needsPrims = false;
		this.componentDescriptor = 0L;
		this.fieldDescriptors = componentMetaToDescriptor(e, null            );
		this.childMetas       = componentMetaToObjectMeta(e, fieldDescriptors);
	}

	Class<?> type(final int index) { return types[1==types.length?0:index]; }
	long getChildDescriptor(final int index) { return (this.metaType == TYPE_MAP ? this.componentDescriptor : (fieldDescriptors == null ? 0L : fieldDescriptors[index])); }
	long getComponentDescriptor() { return componentDescriptor; }

	static ObjectMeta ofPojo(final InternalEngine engine, final Class<?> c, final int cacheIndex) {
		if(c.getCanonicalName().startsWith("java.lang.String")) throw new IllegalStateException(c.getCanonicalName());
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
			final var props        = pojoProps(c, params);
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
				finalComps[targetIdx] = new ComponentMeta(jsonKey, prop.type(), prop.valueType());
				finalTypes[targetIdx] = prop.type();
				if (prop.ctorIdx != -1) ctorTypes[prop.ctorIdx] = prop.type();
				else propDefsList.add(new ConstructorGenerator.PropDef(prop.name(), prop.type(), prop.isField())); // Important: Bytecode require real Java-Name
			}

			final var keys          = FastKeyTable.build(finalComps);
			final var enumConstants = buildEnumConstants(finalTypes);
			final var propDefs      = propDefsList.isEmpty() ? null : propDefsList.toArray(new ConstructorGenerator.PropDef[0]);
			final var factory       = ConstructorGenerator.generate(c, "<init>", ctorTypes, propDefs);

			// ALLES läuft durch die generierte Hochgeschwindigkeits-Factory (kein NO_FACTORY Bypass mehr).
			return new ObjectMeta(engine, c.getCanonicalName(), NO_POJO_START, factory, ctorArgs, keys, finalTypes, finalComps, enumConstants, cacheIndex);
		} catch (final Exception e) {
			e.printStackTrace();
			return DEFECT_FIRST;
		}
	}

	static ObjectMeta ofRecord(final InternalEngine engine, final Class<? extends Record> c, final int cacheIndex) {
		final var comps = c.getRecordComponents();
		final var metaComps = new ComponentMeta[comps.length];
		final var types = new Class<?>[comps.length];
		try {
			for (var i = 0; i < comps.length; i++) {
				final var comp = comps[i];
				types[i] = comp.getType();
				var name = comp.getName();
				if(comp.getAnnotation(org.suche.json.JsonProperty.class) instanceof final org.suche.json.JsonProperty p && !p.value().isEmpty()) name = p.value();
				metaComps[i] = new ComponentMeta(name, types[i], GernericsHandler.extractValueType(comp.getGenericType(), types[i]));
			}
			return new ObjectMeta(engine, c.getCanonicalName(), NO_POJO_START, ConstructorGenerator.generate(c, types), comps.length, FastKeyTable.build(metaComps), types, metaComps, buildEnumConstants(types), cacheIndex);
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

	private static LinkedHashMap<String, Prop> pojoProps(final Class<?> c, final Parameter[] params) throws IllegalAccessException {
		if(c.getCanonicalName().startsWith("java.lang.")) throw new IllegalStateException(c.getCanonicalName());
		final var props = new LinkedHashMap<String, Prop>();

		for (var i = 0; i < params.length; i++) {
			props.put(params[i].getName(), new Prop(params[i].getName(), false, params[i].getType(), GernericsHandler.extractValueType(params[i].getParameterizedType(), params[i].getType()), i, null));
		}

		for (final var e : c.getMethods()) {
			if (e.getParameterCount() != 1 || e.getName().length() < 4 || !e.getName().startsWith("set")) continue;
			final var javaName = e.getName(); // z.B. setNewReg
			var jsonName = Character.toLowerCase(javaName.charAt(3)) + javaName.substring(4);
			if(e.getAnnotation(org.suche.json.JsonProperty.class) instanceof final org.suche.json.JsonProperty p && !p.value().isEmpty()) {
				jsonName = p.value();
			}
			if (!props.containsKey(jsonName)) {
				e.setAccessible(true);
				props.put(jsonName, new Prop(javaName, false, e.getParameterTypes()[0], GernericsHandler.extractValueType(e.getGenericParameterTypes()[0], e.getParameterTypes()[0]), -1, null));
			}
		}

		for (final var e : c.getDeclaredFields()) {
			if (0 != (e.getModifiers() & MOD_FINAL_OR_STATIC)) continue;
			final var javaName = e.getName(); // z.B. newReg
			var jsonName = javaName;
			if(e.getAnnotation(org.suche.json.JsonProperty.class) instanceof final org.suche.json.JsonProperty p && !p.value().isEmpty()) {
				jsonName = p.value();
			}
			if (!props.containsKey(jsonName)) {
				try { e.setAccessible(true); } catch(final Throwable t) { System.out.println(e.getDeclaringClass().getCanonicalName()+"."+e.getName()+" setAccessible(true) => "+t.getMessage()); }
				props.put(jsonName, new Prop(javaName, true, e.getType(), GernericsHandler.extractValueType(e.getGenericType(), e.getType()), -1, null));
			}
		}
		return props;
	}

	ComponentMeta[] components() { return components; }

	int prepareKey(final int hash, final byte[] buffer, final int off, final int len) {
		if (metaType == TYPE_MAP) return -1;
		final var idx = keys.get(hash, buffer, off, len);
		if (idx == -1) {
			if(failOnUnknown) throw new IllegalArgumentException("JSON: Unknown property["+new String(buffer, off, len)+"] in class " + className);
			System.err.println("JSON: Unknown property["+new String(buffer, off, len)+"] in class " + className);
		}
		return idx;
	}

	int prepareKey(final Object context, final String key) {
		if (metaType == TYPE_MAP) {
			if(null == context) throw JsonEngine.illegalStateException("Context must not be null");
			((ParseContext) context).currentKey = key;
			return 0;
		}
		final var b = key.getBytes(StandardCharsets.UTF_8);
		final var idx = keys.get(BufferedStream.computeHash(b, 0, b.length), b, 0, b.length);
		if (idx == -1 && failOnUnknown) throw new IllegalArgumentException("Unknown property " + key + " in class " + className);
		return idx;
	}

	Object start(final MetaPool s) {
		return switch (metaType) {
		case TYPE_MAP, TYPE_OBJ_ARRAY, TYPE_COLLECTION -> s.takeContext(TYPE_MAP == metaType);
		case TYPE_INSTANTIATOR -> {
			final var len = fieldDescriptors != null ? fieldDescriptors.length : 0;
			final var ctx = s.takeContext(false);
			if (ctx.objs == null) ctx.objs = s.takeArray(len);
			if (needsPrims && ctx.prims == null) ctx.prims = s.takeLongArray(len);
			ctx.cnt = len;
			yield ctx;
		}
		case TYPE_SET -> {
			if (baseType != null && baseType != Set.class && !Modifier.isAbstract(baseType.getModifiers()) && !baseType.isInterface()) {
				try {
					yield baseType.getDeclaredConstructor().newInstance();
				} catch (final Exception ex) {
					// Ignored, fallback to HashSet
				}
			}
			yield new HashSet<>();
		}
		default -> throw new IllegalArgumentException();
		};
	}

	private Object endCollection(final MetaPool s, final Object context) {
		final var ctx = (ParseContext) context;
		final var cnt = ctx.cnt;
		if (setEmpty && cnt == 0) { s.returnContext(ctx); return null; }
		if(cnt == 0) {
			s.returnContext(ctx);
			return EmptyJSONArray.ONCE;
		}
		lastSize(s.depth(), cnt);
		if(ctx.prims != null && ctx.prims.length == cnt) {
			if(ctx.singleType == PRIMITIVE.T_LONG || ctx.singleType == PRIMITIVE.T_DOUBLE) {
				final var ret = new CompactList(ctx.singleType, null, ctx.prims);
				ctx.prims = new long[cnt];
				s.returnContext(ctx);
				return ret;
			}
			if(ctx.objs != null && ctx.objs.length == cnt) {
				final var ret = new CompactList(ctx.singleType, ctx.objs, ctx.prims);
				ctx.prims = new long  [cnt];
				ctx.objs  = new Object[cnt];
				s.returnContext(ctx);
				return ret;
			}
		}
		if(ctx.singleType == PRIMITIVE.T_LONG || ctx.singleType == PRIMITIVE.T_DOUBLE) {
			final var ret =  new CompactList(ctx.singleType, null, Arrays.copyOf(ctx.prims, ctx.cnt));
			s.returnContext(ctx);
			return ret;

		}
		final var p = ctx.prims == null ? null : Arrays.copyOf(ctx.prims, ctx.cnt);
		final var o = ctx.objs  == null ? null : Arrays.copyOf(ctx.objs , ctx.cnt);
		final var ret = new CompactList(ctx.singleType, o, p);
		s.returnContext(ctx);
		return ret;
	}

	@SuppressWarnings("unchecked")
	Object end(final MetaPool s, final Object context) throws Throwable {
		return switch (metaType) {
		case TYPE_INSTANTIATOR -> {
			final var ctx = (ParseContext) context;
			final var result = factory.create(ctx.objs, ctx.prims);
			s.returnContext(ctx);
			yield result;
		}
		case TYPE_MAP -> {
			final var ctx = (ParseContext) context;
			if (setEmpty && ctx.cnt == 0) { s.returnContext(ctx); yield null; }
			if (ctx.objs == null) yield null;
			if (baseType != null && baseType != Map.class && !Modifier.isAbstract(baseType.getModifiers()) && !baseType.isInterface() && baseType != CompactMap.class) {
				try {
					@SuppressWarnings("unchecked")
					final var map = (Map<Object, Object>) baseType.getDeclaredConstructor().newInstance();
					for (var i = 0; i < ctx.cnt; i += 2) {
						map.put(ctx.objs[i], ctx.objs[i+1]);
					}
					s.returnContext(ctx);
					yield map;
				} catch (final Exception ex) {
					// Fallback to CompactMap
				}
			}
			final var data = Arrays.copyOf(ctx.objs, ctx.cnt);
			final var prims = ctx.prims == null ? null : Arrays.copyOf(ctx.prims, ctx.cnt >> 1);
			s.returnContext(ctx);
			yield new CompactMap(ctx.singleType, data, prims);
		}
		case TYPE_OBJ_ARRAY -> {
			final var ctx = (ParseContext) context;
			if (setEmpty && ctx.cnt == 0) { s.returnContext(ctx); yield null; }
			lastSize(s.depth(), ctx.cnt);
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
		case TYPE_SET        -> setEmpty && ((Collection<Object>) context).isEmpty() ? null : context;
		case TYPE_SEALED     -> SealedUnionMapper.end(s, context, baseType, permitted, keys, types);
		default -> throw new IllegalArgumentException();
		};
	}

	@SuppressWarnings("unchecked")
	void setLong(final MetaPool s, final Object context, final int index, final long v) {
		if (setNumeric0 && v == 0L) return;
		switch (metaType) {
		case TYPE_INSTANTIATOR               -> ((ParseContext)context).prims[index] = v;
		case TYPE_MAP                        -> ((ParseContext)context).primKeyValue(s, PRIMITIVE.LONG, v);
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> ((ParseContext)context).primIdxValue(s, PRIMITIVE.LONG, v, index);
		case TYPE_SET                        -> ((Collection<Object>) context).add(v);
		default -> set(s, context, index, v);
		}
	}

	@SuppressWarnings("unchecked")
	void setDouble(final MetaPool s, final Object context, final int index, final double v) {
		if (setNumeric0 && v == 0.0) return;
		final var bits = Double.doubleToRawLongBits(v);
		switch (metaType) {
		case TYPE_INSTANTIATOR               -> ((ParseContext)context).prims[index] = bits;
		case TYPE_MAP                        -> ((ParseContext)context).primKeyValue(s, PRIMITIVE.DOUBLE, bits);
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> ((ParseContext)context).primIdxValue(s, PRIMITIVE.DOUBLE, bits, index);
		case TYPE_SET -> ((Collection<Object>) context).add(v);
		default -> set(s, context, index, v);
		}
	}

	void invalidType() { throw new IllegalStateException("metaType: "+metaType); }

	@SuppressWarnings("unchecked")
	void set(final MetaPool s, final Object context, final int index, Object value) {
		// Global null check
		if (value == null && setEmpty && metaType != TYPE_OBJ_ARRAY && metaType != TYPE_COLLECTION) return;
		switch (metaType) {
		case TYPE_INSTANTIATOR -> {
			if (value == null) {
				final var targetType = components[index].type();
				if (targetType.isPrimitive()) value = (targetType == boolean.class ? Boolean.FALSE : 0);
			} else {
				if (components[index].type() == boolean.class) {
					((ParseContext)context).prims[index] = ((Boolean) value) ? 1 : 0;
					return;
				}
				if (this.enumConstants != null && this.enumConstants[index] != null) {
					value = Meta.resolveEnum(this.enumConstants[index], value);
				}
			}
			((ParseContext) context).objs[index] = value;
		}
		case TYPE_MAP -> {
			final var ctx = (ParseContext) context;
			ctx.upgradeToMixed(s, ctx.cnt + 2);
			ctx.objs[ctx.cnt++] = ctx.currentKey;
			ctx.objs[ctx.cnt++] = value;
			ctx.currentKey = null;
		}
		case TYPE_OBJ_ARRAY, TYPE_COLLECTION -> {
			final var ctx = (ParseContext) context;
			ctx.upgradeToMixed(s, index + 1);
			if (ctx.objs == null || index >= ctx.objs.length) ctx.ensureObjs(s, index + 1);
			ctx.objs[index] = value;
			if (index >= ctx.cnt) ctx.cnt = index + 1;
		}
		case TYPE_SET -> ((Collection<Object>) context).add(value);
		default -> invalidType();
		}
	}

}