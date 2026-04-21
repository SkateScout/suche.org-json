package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class JsonInputStream extends BufferedStream implements AutoCloseable, MetaPool {
	private static final byte TYPE_OBJECT = 1;
	private static final byte TYPE_ARRAY  = 2;
	private static final byte TYPE_COL    = 3;
	private static final ObjectPool<JsonInputStream> STREAM_POOL = new ObjectPool<>(64, JsonInputStream::new);
	private static final ObjectPool<CtxStack>        STACK_POOL  = new ObjectPool<>(64, CtxStack::new);

	static final class CTX {
		byte     type;
		Object   obj;
		Object   meta;
		int      targetIdx;
		Class<?> targetType;

		void set(final byte type, final Object obj, final Object meta, final int targetIdx, final Class<?> targetType) {
			this.type       = type;
			this.obj        = obj;
			this.meta       = meta;
			this.targetIdx  = targetIdx;
			this.targetType = targetType;
		}

		void clear() {
			this.obj        = null;
			this.meta       = null;
			this.targetType = null;
		}
	}

	static final class CtxStack {
		CTX[] stack;
		int depth = -1;

		CtxStack() {
			stack = new CTX[64];
			for (var i = 0; i < 64; i++) stack[i] = new CTX();
		}

		void push(final byte type, final Object obj, final Object meta, final int targetIdx, final Class<?> targetType) {
			if (++depth >= stack.length) {
				final var oldLen = stack.length;
				stack = Arrays.copyOf(stack, oldLen * 2);
				for (var i = oldLen; i < stack.length; i++) stack[i] = new CTX();
			}
			stack[depth].set(type, obj, meta, targetIdx, targetType);
		}

		void clear() {
			for (var i = 0; i <= depth; i++) stack[i].clear();
			depth = -1;
		}
	}

	private ObjectMeta defaultMapMeta;
	private CtxStack stack;

	private JsonInputStream() {}

	static JsonInputStream of(final InputStream in, final InternalEngine engine) {
		final var s = STREAM_POOL.acquire();
		s.init(in, engine);
		s.stack = STACK_POOL.acquire();
		s.stack.depth = -1;
		s.defaultMapMeta = engine.metaOf(Map.class);
		return s;
	}

	@Override public void close() throws IOException {
		if (stack != null) {
			stack.clear();
			STACK_POOL.release(stack);
			stack = null;
		}
		depth = -1;
		if (internPool != null) internPool.clear();
		onceInternal.clear();
		final var oldIn = this.in;
		this.in = null;
		STREAM_POOL.release(this);
		if (oldIn != null) oldIn.close();
	}

	ObjectMeta metaOf(final Class<?> clazz) {
		if(engine.metaOf(clazz) instanceof final ObjectMeta r) {
			if(r .components() == null || r.components().length == 0) return r;
			if(onceInternal.add(r))
				for(final var e : r.components()) {
					final var src = e.name().getBytes(StandardCharsets.UTF_8);
					var hash = 0;
					var isAscii = true;
					for (final byte b : src) {
						hash = 31 * hash + b;
						if(b<0) isAscii = false;
					}
					internBytes(src, 0, src.length, hash, isAscii);
				}
			return r;
		}
		throw new IllegalStateException("metaOf("+clazz.getCanonicalName()+")=null");
	}

	private void pushState(final byte type, final Object obj, final Object meta, final int targetIdx, final Class<?> targetType) {
		if (stack.depth + 1 >= maxDepth) throw new IllegalStateException();
		stack.push(type, obj, meta, targetIdx, targetType);
	}

	private Object allocateArrayBuilder(final Class<?> arrayType) {
		return Array.newInstance(arrayType.componentType(), 10);
	}

	private void pushArrayState(final Class<?> targetType) throws IOException {
		pos++;
		if     (targetType != null && targetType.isArray())                   pushState(TYPE_ARRAY, allocateArrayBuilder(targetType), null, 0, targetType  );
		else if(targetType != null && Set.class.isAssignableFrom(targetType)) pushState(TYPE_COL  , new HashSet<>()                 , null, 0, Object.class);
		else                                                                  pushState(TYPE_COL  , takeArray(10)                   , null, 0, Object.class);
	}

	Object finalResult = null;

	private Object parsePrimitiveInline(final byte b, final Class<?> type) throws IOException {
		return switch (b) {
		case '"' -> { pos++; yield parseStringValue(); }
		case 't' -> parseTrue();
		case 'f' -> parseFalse();
		case 'n' -> parseNull();
		default  -> parseNumber(type);
		};
	}

	@SuppressWarnings("unchecked")
	private void applyValueToCurrentState(final Object value) throws IOException {
		if (stack.depth < 0) return;
		final var c = stack.stack[stack.depth];

		switch (c.type) {
		case TYPE_OBJECT -> {
			if (c.targetIdx >= 0) {
				try { ((ObjectMeta) c.meta).set(this, c.obj, c.targetIdx, value); }
				catch (final RuntimeException e) {
					e.printStackTrace(System.out);
					throw e; }
				catch (final Throwable        e) { throw new RuntimeException(e); }
			}
		}
		case TYPE_COL    -> {
			if (c.obj instanceof Object[] array) {
				if (c.targetIdx >= array.length) {
					final var newArray = takeArray(array.length * 2);
					System.arraycopy(array, 0, newArray, 0, array.length);
					returnArray(array);
					c.obj = newArray;
					array = newArray;
				}
				array[c.targetIdx] = value;
				c.targetIdx++;
			} else {
				final var col = (java.util.Collection<Object>) c.obj;
				if (col.size() >= maxCollectionSize) throw new IllegalStateException();
				col.add(value);
			}
		}
		case TYPE_ARRAY  -> appendToArrayBuilder(c, value);
		default          -> { }
		}
	}

	private void applyValue(final Object result) throws IOException {
		applyValueToCurrentState(result);
		consumeCommaIfPresent();
	}

	private void popAndApply(final Object result) throws IOException {
		stack.stack[stack.depth].clear();
		stack.depth--;
		if (stack.depth >= 0) {
			applyValue(result);
		} else {
			this.finalResult = result;
		}
	}

	private Object buildFinalArray(final Object builder, final Class<?> arrayType) {
		final var c = stack.stack[stack.depth];
		final var finalArray = Array.newInstance(arrayType.componentType(), c.targetIdx);
		System.arraycopy(builder, 0, finalArray, 0, c.targetIdx);
		return finalArray;
	}

	private void appendToArrayBuilder(final CTX c, final Object value) {
		var array = c.obj;
		final var len = Array.getLength(array);

		if (c.targetIdx == len) {
			final var newArray = Array.newInstance(c.targetType.componentType(), len * 2);
			System.arraycopy(array, 0, newArray, 0, len);
			c.obj = newArray;
			array = newArray;
		}

		Array.set(array, c.targetIdx, value);
		c.targetIdx++;
	}

	private void consumeCommaIfPresent() throws IOException {
		if (pos < limit && buffer[pos] == (byte) ',') { pos++; return; }
		skipWhitespace();
		if (pos < limit && buffer[pos] == (byte) ',') pos++;
	}

	private Object runStateEngine(final byte startType, final Object startObj, final Object startMeta, final int startIdx, final Class<?> startTarget) throws Throwable {
		pushState(startType, startObj, startMeta, startIdx, startTarget);
		Class<?> targetType = null;
		finalResult = null;
		while (stack.depth >= 0) {
			if (pos < limit) {
				final var b = buffer[pos];
				if (b == 0x20 || b == 0x09 || b == 0x0A || b == 0x0D) skipWhitespace();
			} else skipWhitespace();

			final var c = stack.stack[stack.depth];
			final var shouldContinue = switch (c.type) {
			case TYPE_OBJECT           -> {
				if (peek() == (byte) '}') {
					read();
					popAndApply(((ObjectMeta) c.meta).end(this, c.obj));
					yield true;
				}
				final var meta = (ObjectMeta) c.meta;
				expect((byte) '"');
				final var key = parseStringKey();
				if (pos < limit && buffer[pos] == (byte) ':') pos++;
				else { skipWhitespace(); expect((byte) ':'); }
				c.targetIdx = meta.prepareKey(c.obj, key);
				if (c.targetIdx < 0) {
					skipWhitespace();
					skipValue();
					consumeCommaIfPresent();
					yield true;
				}
				targetType = (c.targetIdx >= 0 ? meta.type(c.targetIdx) : null);
				yield false;
			}
			case TYPE_ARRAY, TYPE_COL  ->  {
				if (peek() == (byte) ']') {
					read();
					if (c.type == TYPE_ARRAY) {
						finalResult = buildFinalArray(c.obj, c.targetType);
					} else if (c.obj instanceof final Object[] array) {
						finalResult = new CompactList<>(Arrays.copyOf(array, c.targetIdx));
						returnArray(array);
					} else finalResult = c.obj;
					popAndApply(finalResult);
					yield true;
				}
				targetType = (c.type == TYPE_ARRAY ? c.targetType.componentType() : c.targetType);
				yield false;
			}
			default                    -> throw new IllegalStateException();
			};

			if (shouldContinue) continue;
			if (pos >= limit || buffer[pos] <= ' ') skipWhitespace();
			if (pos >= limit) throw new IllegalStateException("Unexpected EOF");
			final var b = buffer[pos];

			if (c.type == TYPE_OBJECT && targetType != null && targetType.isPrimitive() && c.targetIdx >= 0) {
				final var meta = (ObjectMeta) c.meta;
				if (b == 'n') {
					parseNull();
					if      (targetType == boolean.class)                             meta.setBoolean(this, c.obj, c.targetIdx, false);
					else if (targetType == double.class || targetType == float.class) meta.setDouble (this, c.obj, c.targetIdx, 0.0);
					else                                                              meta.setLong   (this, c.obj, c.targetIdx, 0L);
				} else if (b == 't' || b == 'f')                                      meta.setBoolean(this, c.obj, c.targetIdx, parseBooleanPrimitive());
				else   if (b == '-' || (b >= '0' && b <= '9')) {
					if (targetType == double.class || targetType == float.class) meta.setDouble(this, c.obj, c.targetIdx, parseDoublePrimitive());
					else                                                         meta.setLong  (this, c.obj, c.targetIdx, parseLongPrimitive());
				} else {
					Object v;
					if (b == '"') {
						if(!engine.skipInvalid()) throw new IllegalStateException("Receive STRING instead of "+targetType.getCanonicalName()+" for "+meta.components()[c.targetIdx].name()+" in "+c.targetType.getCanonicalName());
						pos++;
						v = parseStringValue();
					} else v = parsePrimitiveInline(b, targetType);
					applyValue(v);
					continue;

				}
				consumeCommaIfPresent();
				continue;
			}
			switch (b) {
			case '{' -> {
				pos++;
				final var t = targetType == null ? Map.class : targetType;
				final var meta = (t == Map.class) ? defaultMapMeta : metaOf(t);
				pushState(TYPE_OBJECT, meta.start(this), meta, -1, t);
			}
			case '[' -> pushArrayState(targetType);
			case '"' -> { pos++; applyValue(parseStringValue()); }
			case 't' -> applyValue(parseTrue());
			case 'f' -> applyValue(parseFalse());
			case 'n' -> applyValue(parseNull());
			default  -> applyValue(parseNumber(targetType));
			}
		}
		return finalResult;
	}

	// ========================================================================
	// PUBLIC API
	// ========================================================================

	public <T> T readObject(final Class<T> targetClass) throws IOException {
		skipWhitespace();
		expect((byte) '{');
		final var meta = (targetClass == Map.class) ? defaultMapMeta : metaOf(targetClass);
		try {
			final var r = (engine.maxRecursiveDepth() <= 0
					? runStateEngine(TYPE_OBJECT, meta.start(this), meta, -1, targetClass)
							: parseRecordRecursive(meta, 0));
			return targetClass.cast(r);
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> T[] readRecords(final Class<T> targetClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		try {
			return (T[]) (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(TYPE_ARRAY, allocateArrayBuilder(targetClass), null, 0, targetClass)
					: parseArrayRecursive(targetClass, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <V> Map<String, V> readMap(final Class<V> valueClass) throws IOException {
		skipWhitespace();
		expect((byte) '{');
		final var meta = new ObjectMeta(valueClass);
		try {
			return (Map<String, V>) (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(TYPE_OBJECT, meta.start(this), meta, -1, valueClass)
					: parseRecordRecursive(meta, 0));

		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> readList(final Class<T> elementClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		try {
			return (List<T>) (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(TYPE_COL, takeArray(10), null, 0, elementClass)
					: parseArrayRecursive(elementClass, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> Set<T> readSet(final Class<T> elementClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		try {
			return (Set<T>)  (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(TYPE_COL, new HashSet<>(), null, 0, elementClass)
					: parseCollectionRecursive(new HashSet<>(), elementClass, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	// ========================================================================
	// RECURSIVE FAST-PATH ENGINE (Mit Hybrid-Fallback zur State-Machine)
	// ========================================================================

	private Object parseValueRecursive(final Class<?> type, final int recursionDeep) throws Throwable {
		skipWhitespace();
		if (pos >= limit) throw new IllegalStateException("Unexpected EOF");
		final var b = buffer[pos];

		// --- Change to state engine parser
		if (recursionDeep >= this.maxDepth) {
			if (b == '{') {
				pos++;
				final var t = type == null ? Map.class : type;
				final var meta = (t == Map.class) ? defaultMapMeta : metaOf(type);
				return runStateEngine(TYPE_OBJECT, meta.start(this), meta, -1, t);
			}
			if (b == '[') {
				pos++;
				if (type != null && Set.class.isAssignableFrom(type)) {
					return runStateEngine(TYPE_COL, new HashSet<>(), null, 0, Object.class);
				}
				final var compType = (type != null && type.isArray()) ? type.componentType() : type;
				final var startObj   = (type != null && type.isArray()) ? allocateArrayBuilder(type) : takeArray(10);
				return runStateEngine((type != null && type.isArray()) ? TYPE_ARRAY : TYPE_COL, startObj, null, 0, compType);
			}
		}
		switch (b) {
		case '{': pos++; return parseRecordRecursive(metaOf(type != null ? type : Map.class), recursionDeep);
		case '[': pos++; if (type != null && Set.class.isAssignableFrom(type))  return parseCollectionRecursive(new HashSet<>(), Object.class, recursionDeep); return parseArrayRecursive(type, recursionDeep);
		case '"': pos++; return parseStringValue();
		case 't': return parseTrue();
		case 'f': return parseFalse();
		case 'n': return parseNull();
		default: break;
		}
		return parseNumber(type);
	}

	private Object parseRecordRecursive(final ObjectMeta meta, final int recursionDeep) throws Throwable {
		final var context = meta.start(this);
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) '}') { pos++; return meta.end(this, context); }
			expect((byte) '"');
			final var key = parseStringKey();
			if (pos < limit && buffer[pos] == (byte) ':') pos++;
			else { skipWhitespace(); expect((byte) ':'); }
			final var targetIdx = meta.prepareKey(context, key);
			if (targetIdx < 0) { skipWhitespace(); skipValue(); consumeCommaIfPresent(); continue; }
			final var type = targetIdx >= 0 ? meta.type(targetIdx) : null;
			skipWhitespace();
			final var b = buffer[pos];
			if (type != null && type.isPrimitive()) {
				if (b == '"') throw new IllegalArgumentException("Type mismatch: Expected primitive number/boolean, got String at pos " + pos);
				if (b == '{' || b == '[') throw new IllegalArgumentException("Type mismatch: Expected primitive, got Object/Array at pos " + pos);
				if (type == int.class || type == long.class || type == short.class || type == byte.class) {
					meta.setLong(this, context, targetIdx, parseLongPrimitive());
				} else if (type == double.class || type == float.class) {
					meta.setDouble(this, context, targetIdx, parseDoublePrimitive());
				} else if (type == boolean.class) {
					meta.setBoolean(this, context, targetIdx, parseBooleanPrimitive());
				} else {
					meta.set(this, context, targetIdx, parsePrimitiveInline(b, type));
				}
			} else {

				final Object value;
				if (recursionDeep >= this.maxDepth) {
					// State-Fallback
					if (b == '{') {
						pos++;
						final var t = type == null ? Map.class : type;
						final var fallbackMeta = t == Map.class ? defaultMapMeta : metaOf(t);
						value = runStateEngine(TYPE_OBJECT, fallbackMeta.start(this), fallbackMeta, -1, t);
					} else if (b == '[') {
						pos++;
						if (type != null && Set.class.isAssignableFrom(type)) {
							value = runStateEngine(TYPE_COL, new HashSet<>(), null, 0, Object.class);
						} else {
							final var compType = (type != null && type.isArray()) ? type.componentType() : type;
							final var startObj = (type != null && type.isArray()) ? allocateArrayBuilder(type) : takeArray(10);
							value = runStateEngine((type != null && type.isArray()) ? TYPE_ARRAY : TYPE_COL, startObj, null, 0, compType);
						}
					} else {
						value = parsePrimitiveInline(b, type);
					}
				} else {
					switch (b) {
					case '{' -> { pos++; value = parseRecordRecursive(type == null || type == Map.class ? defaultMapMeta : metaOf(type), recursionDeep + 1); }
					case '[' -> { pos++; value = (type != null && Set.class.isAssignableFrom(type)) ? parseCollectionRecursive(new HashSet<>(), Object.class, recursionDeep + 1) : parseArrayRecursive(type, recursionDeep + 1); }
					default  -> { value = parsePrimitiveInline(b, type); }
					}
				}
				if (targetIdx >= 0) meta.set(this, context, targetIdx, value);
			}
			consumeCommaIfPresent();
		}
	}

	private Object parseArrayRecursive(final Class<?> targetType, final int recursionDeep) throws Throwable {
		final var isArray = targetType != null && targetType.isArray();
		final var compType = isArray ? targetType.componentType() : (targetType != null ? targetType : Object.class);
		var builder = isArray ? allocateArrayBuilder(targetType) : takeArray(10);
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') {
				pos++;
				if (isArray) {
					final var finalArray = Array.newInstance(compType, idx);
					System.arraycopy(builder, 0, finalArray, 0, idx);
					return finalArray;
				}
				final var array = (Object[]) builder;
				final var list = new CompactList<>(java.util.Arrays.copyOf(array, idx));
				returnArray(array);
				return list;
			}
			final var value = parseValueRecursive(compType, recursionDeep + 1);
			if (isArray) {
				if (idx == Array.getLength(builder)) {
					final var newArray = Array.newInstance(compType, idx * 2);
					System.arraycopy(builder, 0, newArray, 0, idx);
					builder = newArray;
				}
				Array.set(builder, idx++, value);
			} else {
				var array = (Object[]) builder;
				if (idx == array.length) {
					final var newArray = takeArray(array.length * 2);
					System.arraycopy(array, 0, newArray, 0, array.length);
					returnArray(array);
					builder = newArray;
					array = newArray;
				}
				array[idx++] = value;
			}
			consumeCommaIfPresent();
		}
	}

	private Object parseCollectionRecursive(final java.util.Collection<Object> builder, final Class<?> compType, final int recursionDeep) throws Throwable {
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') {
				pos++;
				return builder;
			}
			builder.add(parseValueRecursive(compType, recursionDeep + 1));
			consumeCommaIfPresent();
		}
	}
}