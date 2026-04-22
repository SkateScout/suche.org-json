// text
package org.suche.json;

import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class SealedUnionMapper {
	static final String CLASS_KEY = "__class__";
	static final String ENUM_KEY  = "__enum__";

	private static final Set<Class<?>> TOP  = Set.of(Object.class);
	private static final int           LEAF = Modifier.ABSTRACT | Modifier.INTERFACE;

	private static Set<Class<?>> getAllSuperTypes(final Class<?> c) {
		final var result = new HashSet<Class<?>>();
		final var queue  = new ArrayDeque<Class<?>>();
		queue.add(c);
		while (!queue.isEmpty()) {
			final var current = queue.poll();
			if (result.add(current)) {
				if (current.getSuperclass() != null) queue.add(current.getSuperclass());
				Collections.addAll(queue, current.getInterfaces());
			}
		}
		return result;
	}

	private static Set<Class<?>> getCommonSuperTypes(final Set<Class<?>> types) {
		if (types == null || types.isEmpty()) return TOP;
		if(types.size() == 1) return types;
		final var iterator = types.iterator();
		final var common   = getAllSuperTypes(iterator.next());
		while (iterator.hasNext()) common.retainAll(getAllSuperTypes(iterator.next()));
		common.removeIf(type -> {
			for (final Class<?> other : common) if (type != other && type.isAssignableFrom(other)) return true;
			return false;
		});
		if (!common.isEmpty()) return common;
		return TOP;
	}

	private static Class<?>[] getPermittedClasses(final Class<?> baseClass) {
		final var r = new HashSet   <Class<?>>();
		final var q = new ArrayDeque<Class<?>>();
		q.add(baseClass);
		while(q.poll() instanceof final Class<?> c) {
			if (0 == (c.getModifiers() & LEAF)) r.add(c);
			if(c.getPermittedSubclasses() instanceof final Class<?>[] s) Collections.addAll(q, s);
		}
		return r.toArray(new Class<?>[0]);
	}

	static ObjectMeta build(final InternalEngine e, final Class<?> sealedInterface) {
		final var subclasses    = getPermittedClasses(sealedInterface);
		final var keyToAllTypes = new HashMap<String, Set<Class<?>>>();
		for (final var sub : subclasses) {
			final var meta = e.metaOf(sub);
			for (final var comp : meta.components) keyToAllTypes.computeIfAbsent(comp.name(), _ -> new HashSet<>()).add(comp.type());
		}
		var totalKeys = 1;
		final var keyToIndex = new HashMap<String, Integer>();
		final var typeMap    = new HashMap<Integer, Class<?>>();
		for (final var entry : keyToAllTypes.entrySet()) {
			final var key = entry.getKey();
			if(CLASS_KEY.equals(key) || ENUM_KEY .equals(key)) continue;
			final var idx = totalKeys++;
			keyToIndex.put(entry.getKey(), idx);
			final var common = getCommonSuperTypes(entry.getValue());
			typeMap.put(idx, common.size() == 1 ? common.iterator().next() : Object.class);
		}

		final var unionKeys  = new String  [totalKeys];
		final var unionTypes = new Class<?>[totalKeys];
		for (final var entry : keyToIndex.entrySet()) {
			unionKeys [entry.getValue()] = entry.getKey();
			unionTypes[entry.getValue()] = typeMap.get(entry.getValue());
		}
		unionKeys [0] = CLASS_KEY;
		unionTypes[0] = String.class;
		return new ObjectMeta(e, sealedInterface.getCanonicalName(), subclasses, unionKeys, unionTypes, e.failOnUnknownProperties());
	}

	static Object end(final MetaPool s, final Object context, final Class<?> baseType, final Class<?>[] permitted, final FastKeyTable keys, final Class<?>[] unionTypes) throws Throwable {
		if(null == permitted) throw new IllegalStateException("permitted=null");
		final var unionCtx  = (MetaPool.ParseContext) context;
		final var unionObj  = unionCtx.objs;
		final var unionPrim = unionCtx.prims;
		var className = (String) unionObj[0];
		if (className == null) { final var enumIdx = keys.get(ENUM_KEY); if (enumIdx >= 0) className = (String) unionObj[enumIdx]; }
		final var targetClass = resolvePermittedSubclass(baseType, permitted, className);
		final var engine      = s.engine();
		final var targetMeta  = engine.metaOf(targetClass);
		final var targetCtx   = s.takeContext(targetMeta.components.length);
		targetCtx.cnt         = targetMeta.components.length;
		final var targetObj   = targetCtx.objs;
		final var targetPrim  = targetCtx.prims;


		for (var i = 0; i < targetMeta.components.length; i++) {
			final var comp = targetMeta.components[i];
			final var unionIdx = keys.get(comp.name());
			if (unionIdx < 0) continue;
			final var uType = unionTypes[unionIdx];

			if (uType.isPrimitive()) {
				targetPrim[i] = unionPrim[unionIdx];
			} else {
				var val = unionObj[unionIdx];
				if (val != null) {
					if (val instanceof final CompactMap<?,?> m && comp.type() != Object.class && comp.type() != Map.class) {
						val = applyLateBindingIterative(s, m, comp.type());
					} else if (val instanceof final CompactList<?> l && comp.type().isArray()) {
						val = applyLateBindingIterative(s, l, comp.type());
					} else {
						val = coercePrimitive(val, comp.type());
					}
				}

				if (comp.type().isPrimitive()) {
					var primVal = 0L;
					if (val instanceof final Boolean b) primVal = b ? 1L : 0L;
					else if (val instanceof final Number n) {
						if      (comp.type() == double.class) primVal = Double.doubleToRawLongBits(n.doubleValue());
						else if (comp.type() == float .class) primVal = Float.floatToRawIntBits(n.floatValue());
						else                                  primVal = n.longValue();
					}
					targetPrim[i] = primVal;
				} else {
					var objVal = val;
					if (targetMeta.enumConstants != null && targetMeta.enumConstants[i] != null && objVal != null) {
						objVal = Meta.resolveEnum(targetMeta.enumConstants[i], objVal);
					}
					targetObj[i] = objVal;
				}
			}
		}

		// MAXIMALE VEREINFACHUNG: Die Factory erledigt nun Instanziierung UND Setter!
		final var result = targetMeta.factory.create(targetObj, targetPrim);

		s.returnContext(targetCtx);
		s.returnContext(unionCtx);
		return result;
	}

	private static Class<?> resolvePermittedSubclass(final Class<?> baseType, final Class<?>[] permitted, final String className) {
		if(null == permitted) throw new IllegalStateException("permitted=null");
		if (className == null) {
			if(baseType==null) throw new IllegalArgumentException("className and baseType is null");
			return baseType;
		}
		for (final var c : permitted) if (c.getSimpleName().equals(className) || c.getName().equals(className)) return c;
		throw new IllegalArgumentException("Unknown subclass: " + className + " for " + Arrays.toString(permitted));
	}

	private static ObjectMeta getMeta(final InternalEngine engine, final Object[] raw, final Class<?> targetType) {
		String className = null;
		for (var i = 0; i < raw.length - 1; i += 2) if (CLASS_KEY.equals(raw[i]) || ENUM_KEY.equals(raw[i])) { className = (String) raw[i + 1]; break; }
		Class<?> concreteClass = targetType;
		if (className != null) {
			final var baseMeta = engine.metaOf(targetType);
			if (baseMeta != null && baseMeta.permitted != null) concreteClass = resolvePermittedSubclass(targetType, baseMeta.permitted, className);
		}
		return engine.metaOf(concreteClass);
	}

	static final class CTX {
		Object[]   raw;
		long[]     prims;
		ObjectMeta meta;
		Object     context;
		int        idx;
		int        len;
		boolean    isList;
		Class<?>   compType;
		int        parentTargetIdx;

		void seti(final Object[] raw, final long[] prims, final ObjectMeta meta, final Object context, final Class<?> compType, final int parentTargetIdx) {
			this.raw             = raw             ;
			this.prims           = prims           ;
			this.meta            = meta            ;
			this.context         = context         ;
			this.idx             = 0               ;
			this.len             = raw.length      ;
			this.isList          = null != compType;
			this.compType        = compType        ;
			this.parentTargetIdx = parentTargetIdx ;
		}

		void clear() { raw = null; prims = null; meta = null; context = null; compType = null; }
	}

	static final class CtxStack {
		CTX[] stack;
		int depth ;
		CtxStack() { stack = new CTX[64]; for (var i = 0; i < 64; i++) stack[i] = new CTX(); }
		void ensureCapacity(final int requestedDepth) {
			if (requestedDepth >= stack.length) {
				final var oldLen = stack.length;
				stack = Arrays.copyOf(stack, requestedDepth * 2);
				for (var i = oldLen; i < stack.length; i++) stack[i] = new CTX();
			}
		}
		CtxStack init() { depth = -1; return this; }

		void push(final Object[] raw, final long[] prims, final ObjectMeta meta, final Object context, final Class<?> compType, final int parentTargetIdx) {
			depth++;
			ensureCapacity(depth);
			stack[depth].seti(raw, prims, meta, context, compType, parentTargetIdx);
		}
	}

	private static final org.suche.json.ObjectPool<CtxStack> REVISION_POOL = new org.suche.json.ObjectPool<>(64, CtxStack::new);

	private static Object applyLateBindingIterative(final MetaPool s, final Object rootVal, final Class<?> rootExpected) throws Throwable {
		if (!(rootVal instanceof CompactMap) && !(rootVal instanceof CompactList)) return coercePrimitive(rootVal, rootExpected);
		final var stack = REVISION_POOL.acquire().init();
		Object finalResult = null;
		final var engine = s.engine();
		try {
			if (rootVal instanceof final CompactMap<?, ?> m) {
				final var raw = m.getRawData();
				final var prims = m.prims;
				final var meta = getMeta(engine, raw, rootExpected);
				if (meta == null || meta == ObjectMeta.GENERIC_MAP || meta == ObjectMeta.NULL) return rootVal;
				stack.push(raw, prims, meta, meta.start(s), null, -1);
			} else if (rootVal instanceof final CompactList<?> l) {
				final var raw = l.getRawData();
				final var context = Array.newInstance(rootExpected.componentType(), raw.length);
				stack.push(raw, null, null, context, rootExpected.componentType(), -1);
			}
			while (stack.depth >= 0) {
				final var c = stack.stack[stack.depth];
				if (c.isList) {
					if (c.idx < c.len) {
						final var val = c.raw[c.idx];
						final var currentIdx = c.idx++;
						if (val instanceof final CompactMap<?, ?> m && c.compType != Object.class && c.compType != java.util.Map.class) {
							final var childRaw = m.getRawData();
							final var childPrims = m.prims;
							final var childMeta = getMeta(engine, childRaw, c.compType);
							if (childMeta == null || childMeta == ObjectMeta.GENERIC_MAP || childMeta == ObjectMeta.NULL) {
								Array.set(c.context, currentIdx, m);
							} else {
								stack.push(childRaw, childPrims, childMeta, childMeta.start(s), null, currentIdx);
							}
						} else if (val instanceof final CompactList<?> l && c.compType.isArray()) {
							final var childRaw = l.getRawData();
							stack.push(childRaw, null, null, Array.newInstance(c.compType.componentType(), childRaw.length), c.compType.componentType(), currentIdx);
						} else {
							Array.set(c.context, currentIdx, coercePrimitive(val, c.compType));
						}
					} else {
						finalResult = c.context;
						stack.depth--;
						if (stack.depth >= 0) {
							final var parent = stack.stack[stack.depth];
							if (parent.isList) Array.set(parent.context, c.parentTargetIdx, finalResult);
							else parent.meta.set(s, parent.context, c.parentTargetIdx, finalResult);
						}
						c.clear();
					}
				} else {
					var pushed = false;
					while (c.idx < c.len) {
						final var key = (String) c.raw[c.idx];
						var val = c.raw[c.idx + 1];

						// LAZY PRIMITIVES AUFLÖSEN
						if (val == CompactMap.PRIMITIVE.LONG)        val = c.prims[(c.idx + 1) >> 1];
						else if (val == CompactMap.PRIMITIVE.DOUBLE) val = Double.longBitsToDouble(c.prims[(c.idx + 1) >> 1]);

						c.idx += 2;

						if (CLASS_KEY.equals(key)) continue;
						final var targetIdx = c.meta.prepareKey(c.context, key);
						if (targetIdx >= 0) {
							final Class<?> expectedType = c.meta.type(targetIdx);
							if (val instanceof final CompactMap<?, ?> m && expectedType != Object.class && expectedType != java.util.Map.class) {
								final var childRaw = m.getRawData();
								final var childPrims = m.prims;
								final var childMeta = getMeta(engine, childRaw, expectedType);
								if ((childMeta != null) && (childMeta != ObjectMeta.GENERIC_MAP) && (childMeta != ObjectMeta.NULL)) {
									stack.push(childRaw, childPrims, childMeta, childMeta.start(s), null, targetIdx);
									pushed = true;
									break;
								}
								c.meta.set(s, c.context, targetIdx, val);
							} else if (val instanceof final CompactList<?> l && expectedType.isArray()) {
								final var childRaw = l.getRawData();
								stack.push(childRaw, null, null, Array.newInstance(expectedType.componentType(), childRaw.length), expectedType.componentType(), targetIdx);
								pushed = true;
								break;
							} else {
								c.meta.set(s, c.context, targetIdx, coercePrimitive(val, expectedType));
							}
						}
					}

					if (!pushed && c.idx >= c.len) {
						finalResult = c.meta.end(s, c.context);
						stack.depth--;
						if (stack.depth >= 0) {
							final var parent = stack.stack[stack.depth];
							if (parent.isList) Array.set(parent.context, c.parentTargetIdx, finalResult);
							else parent.meta.set(s, parent.context, c.parentTargetIdx, finalResult);
						}
						c.clear();
					}
				}
			}
		} finally { REVISION_POOL.release(stack); }
		return finalResult;
	}

	private static Object coercePrimitive(final Object val, final Class<?> expected) {
		if (val instanceof final Number n) {
			if (expected == int   .class || expected == Integer.class) return n.intValue   ();
			if (expected == long  .class || expected == Long   .class) return n.longValue  ();
			if (expected == float .class || expected == Float  .class) return n.floatValue ();
			if (expected == double.class || expected == Double .class) return n.doubleValue();
			if (expected == short .class || expected == Short  .class) return n.shortValue ();
			if (expected == byte  .class || expected == Byte   .class) return n.byteValue  ();
		}
		return val;
	}
}