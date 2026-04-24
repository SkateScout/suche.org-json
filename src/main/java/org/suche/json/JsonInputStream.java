package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.suche.json.ObjectMeta.ComponentMeta;

public final class JsonInputStream extends BufferedStream implements AutoCloseable, MetaPool {
	private static final ObjectPool<JsonInputStream> STREAM_POOL = new ObjectPool<>(64, JsonInputStream::new);

	public ObjectMeta[] metaCache;

	static final class CTX  {
		long   typeDesc;
		Object obj;
		Object meta;
		int    targetIdx;

		void set(final long pTypeDesc, final Object pObj, final Object pMeta, final int pTargetIdx) {
			this.typeDesc  = pTypeDesc;
			this.obj       = pObj;
			this.meta      = pMeta;
			this.targetIdx = pTargetIdx;
		}

		public void clean() {
			this.obj  = null;
			this.meta = null;
		}
	}

	static final class CtxStack {
		private final CTX[] stack64 = new CTX[64];
		CTX[] stack = stack64;
		int   depth = -1;

		CtxStack() { for (var i = 0; i < 64; i++) stack[i] = new CTX(); }

		void push(final long typeDesc, final Object obj, final Object meta, final int targetIdx) {
			if (++depth >= stack.length) {
				final var oldLen = stack.length;
				stack = Arrays.copyOf(stack, oldLen * 2);
				for (var i = oldLen; i < stack.length; i++) stack[i] = new CTX();
			}
			stack[depth].set(typeDesc, obj, meta, targetIdx);
		}

		void clear() {
			for (var i = 0; i <= depth; i++) stack[i].clean();
			if (stack.length > 64) stack = stack64;
			depth = -1;
		}
	}

	private ObjectMeta defaultMapMeta;
	private final CtxStack engineStack = new CtxStack();

	private Class<?>   lastResolvedClass;
	private ObjectMeta lastResolvedMeta;
	private int        lastArraySize = 16;

	private JsonInputStream() {}

	static JsonInputStream of(final InputStream in, final InternalEngine engine) {
		final var s = STREAM_POOL.acquire();
		s.init(in, engine);
		s.engineStack.depth = -1;
		s.lastArraySize = 16;
		final var e = (EngineImpl) engine;
		s.defaultMapMeta = e.defaultMapMeta;
		s.metaCache = engine.metaCache();
		return s;
	}

	static long createTypeDesc(final boolean isArray, final boolean isPrimitive, final int cacheIndex) {
		var desc = ((long) cacheIndex) << 1;
		if (isArray)     desc |= 0x8000000000000000L;
		if (isPrimitive) desc |= 1L;
		return desc;
	}

	@Override public void close() throws IOException {
		engineStack.clear();
		depth             = -1;
		lastResolvedClass = null;
		lastResolvedMeta  = null;
		if (internPool != null) internPool.clear();
		Arrays.fill(onceInternal, 0, onceInternalSize, null);
		onceInternalSize = 0;
		final var oldIn = this.in;
		this.in = null;
		STREAM_POOL.release(this);
		if (oldIn != null) oldIn.close();
	}

	private void internalComponentKeys(final ComponentMeta[] components) {
		for(final var e : components) {
			final var src = e.name().getBytes(StandardCharsets.UTF_8);
			var hash = 0;
			var isAscii = true;
			for (final byte b : src) {
				hash = 31 * hash + b;
				if(b < 0) isAscii = false;
			}
			internBytes(src, 0, src.length, hash, isAscii);
		}
	}

	ObjectMeta metaOf(final Class<?> clazz) {
		if(clazz == lastResolvedClass) return lastResolvedMeta;

		if(engine.metaOf(clazz) instanceof final ObjectMeta r) {
			this.lastResolvedClass = clazz;
			this.lastResolvedMeta  = r;

			if(r .components() == null || r.components().length == 0) return r;
			final var arr  = this.onceInternal;
			final var size = this.onceInternalSize;
			for (var i = 0; i < size; i++) if (arr[i] == r) return r;
			if (size < arr.length) {
				arr[this.onceInternalSize++] = r;
				internalComponentKeys(r.components());
			}
			return r;
		}
		throw new IllegalStateException("metaOf("+clazz.getCanonicalName()+")=null");
	}

	Object finalResult = null;

	private void applyValueToCurrentState(final Object value) {
		if (engineStack.depth < 0) return;
		final var c = engineStack.stack[engineStack.depth];
		if (c.targetIdx >= 0 || c.typeDesc < 0L) {
			((ObjectMeta) c.meta).set(this, c.obj, c.targetIdx, value);
			c.targetIdx = c.typeDesc < 0L ? c.targetIdx + 1 : -1;
		}
	}

	private void consumeCommaIfPresent() throws IOException {
		if (pos < limit && buffer[pos] == (byte) ',') { pos++; return; }
		skipWhitespace();
		if (pos < limit && buffer[pos] == (byte) ',') pos++;
	}

	private void fillNullValue(final long typeDesc, final Object obj, final int targetIdx, final ObjectMeta meta, final Class<?> targetType) throws IOException {
		parseNull();
		if (!engine.config().skipDefaultValues() || typeDesc < 0L) {
			if      (targetType == boolean.class)                             meta.set       (this, obj, targetIdx, Boolean.FALSE);
			else if (targetType == double.class || targetType == float.class) meta.setDouble (this, obj, targetIdx, 0.0);
			else if (targetType != null && targetType.isPrimitive())          meta.setLong   (this, obj, targetIdx, 0L);
			else                                                              meta.set       (this, obj, targetIdx, null);
		}
	}

	private Object runStateEngine(final long startTypeDesc, final Object startObj, final Object startMeta, final int startIdx) throws Throwable {
		var       curTypeDesc = startTypeDesc;
		var       curObj      = startObj;
		var       curMeta     = (ObjectMeta) startMeta;
		var       curIdx      = startIdx;

		engineStack.clear();

		while (true) {
			if (pos >= limit) { ensure(1); if (pos >= limit) throw new IllegalStateException("Unexpected EOF"); }
			var b = buffer[pos];

			switch (b) {
			case '\t', '\n', '\r', ' ' -> {
				pos++;
				while (pos < limit) {
					b = buffer[pos];
					if ((b & 0xC0) != 0 || ((1L << b) & WHITESPACE_MASK) == 0) break;
					pos++;
				}
			}
			case '"' -> {
				if (curTypeDesc >= 0L && curIdx < 0) {
					pos++;
					curIdx = parseStringKeyAsIndex(curObj, curMeta);

					if (pos < limit && buffer[pos] == (byte) ':') {
						pos++;
					} else {
						skipWhitespace();
						expect((byte) ':');
					}

					if (curIdx < 0) { skipWhitespace(); skipValue(); consumeCommaIfPresent(); }
				} else {
					curMeta.set(this, curObj, curIdx, parseStringValue());
					curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
					consumeCommaIfPresent();
				}
			}
			case '}', ']' -> {
				final var isArray = curTypeDesc < 0L;
				if ((b == '}') == isArray || (b == '}' && curIdx >= 0)) throw new IllegalStateException("Unexpected character: " + (char) b);
				pos++;
				final var finishedObj = curMeta.end(this, curObj);
				if (engineStack.depth < 0) return finishedObj;
				final var parent = engineStack.stack[engineStack.depth--];
				curTypeDesc = parent.typeDesc;
				curObj      = parent.obj;
				curMeta     = (ObjectMeta) parent.meta;
				curIdx      = parent.targetIdx;
				curMeta.set(this, curObj, curIdx, finishedObj);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case '-','0','1','2','3','4','5','6','7','8','9' -> {
				final var target = curIdx < 0 ? null : curMeta.type(curIdx);
				parseNumericPrimitive(curMeta, curObj, curIdx, target);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case 'n' -> {
				parseNull();
				curMeta.set(this, curObj, curIdx, null);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case 't' -> {
				curMeta.set(this, curObj, curIdx, parseTruePrimitive());
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case 'f' -> {
				curMeta.set(this, curObj, curIdx, parseFalsePrimitive());
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case '{' -> {
				if (curTypeDesc >= 0L && curIdx < 0) throw new IllegalStateException("Expected key");
				pos++;
				final var childDesc = curTypeDesc >= 0L ? curMeta.getChildDescriptor(curIdx) : curMeta.getComponentDescriptor();

				// Validierung: Wenn das Schema ein Array fordert, das JSON aber ein Objekt liefert
				if (childDesc < 0L) throw new IllegalStateException("Expected array, got '{'");
				if ((childDesc & 1L) != 0L) throw new IllegalStateException("Expected primitive, got '{'");

				engineStack.push(curTypeDesc, curObj, curMeta, curIdx);
				curTypeDesc = childDesc;
				curMeta     = metaCache[(int) (curTypeDesc >> 1)];
				curObj      = curMeta.start(this);
				curIdx      = -1;
			}
			case '[' -> {
				if (curTypeDesc >= 0L && curIdx < 0) throw new IllegalStateException("Expected key");
				pos++;
				var childDesc = curTypeDesc >= 0L ? curMeta.getChildDescriptor(curIdx) : curMeta.getComponentDescriptor();
				// Validierung: Wenn das Schema ein Objekt/Map fordert, das JSON aber ein Array liefert
				if (childDesc >= 0L) {
					final var expectedMeta = metaCache[(int) (childDesc >> 1)];
					// Wenn der Zieltyp "Object" (also generisch) ist, switchen wir fliegend auf eine Collection um!
					if (expectedMeta != defaultMapMeta) {
						throw new IllegalStateException("Expected object, got '['");
					}
					final var colIdx = ((EngineImpl)engine).genericCollectionMeta.cacheIndex;
					childDesc = createTypeDesc(true, false, colIdx); // Bit 63 (Array-Flag) sicher setzen!
				}
				if ((childDesc & 1L) != 0L) {
					final var primId = (int) ((childDesc & 0x7FFFFFFFFFFFFFFFL) >> 1);
					final var fallback = curTypeDesc >= 0L ? curMeta.type(curIdx).componentType() : curMeta.type(0).componentType();
					curMeta.set(this, curObj, curIdx, parsePrimitiveArray(primId, fallback));
					curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
					consumeCommaIfPresent();
				} else {
					engineStack.push(curTypeDesc, curObj, curMeta, curIdx);
					curTypeDesc = childDesc;
					curMeta     = metaCache[(int) (curTypeDesc >> 1)];
					curObj      = curMeta.start(this);
					curIdx      = 0;
				}
			}
			default -> throw new IllegalStateException("Unexpected character: " + (char) b);
			}
		}
	}

	// ========================================================================
	// PUBLIC API
	// ========================================================================


	@SuppressWarnings("unchecked")
	public <T> T[] readRecords(final Class<T> targetClass) throws IOException {
		skipWhitespace();
		final var meta = getDynamicMeta(targetClass, ObjectMeta.TYPE_OBJ_ARRAY);
		expect((byte) '[');
		final var typeDesc = createTypeDesc(true, false, meta.cacheIndex);

		try {
			return (T[]) (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, 0)
					: parseArrayRecursive(typeDesc, meta, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <V> Map<String, V> readMap(final Class<V> valueClass) throws IOException {
		skipWhitespace();
		expect((byte) '{');
		final var meta = getDynamicMeta(valueClass, ObjectMeta.TYPE_MAP);
		final var typeDesc = createTypeDesc(false, false, meta.cacheIndex);

		try {
			return (Map<String, V>) (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, -1)
					: parseRecordRecursive(meta, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> readList(final Class<T> elementClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		final var meta = getDynamicMeta(elementClass, ObjectMeta.TYPE_COLLECTION);
		final var typeDesc = createTypeDesc(true, false, meta.cacheIndex);

		try {
			return (List<T>) (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, 0)
					: parseArrayRecursive(typeDesc, meta, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	@SuppressWarnings("unchecked")
	public <T> Set<T> readSet(final Class<T> elementClass) throws IOException {
		skipWhitespace();
		expect((byte) '[');
		final var meta = getDynamicMeta(elementClass, ObjectMeta.TYPE_SET);
		final var typeDesc = createTypeDesc(true, false, meta.cacheIndex);

		try {
			return (Set<T>)  (engine.maxRecursiveDepth() <= 0 ?
					runStateEngine(typeDesc, meta.start(this), meta, 0)
					: parseArrayRecursive(typeDesc, meta, 0));
		} catch(final IOException | RuntimeException e) { throw e;
		} catch(final Throwable e) { throw new RuntimeException(e); }
	}

	public Object readObject(final Class<?> targetType) throws Throwable {
		skipWhitespace();
		if (pos >= limit) return null;

		final var b = buffer[pos];
		if ((b != '{') && (b != '[')) return parsePrimitiveInline(b, targetType);
		pos++;

		var targetMeta = engine.metaOf(targetType);
		var cacheIdx = targetMeta != null ? targetMeta.cacheIndex : 0;
		final var isArray = (b == '[');

		if (isArray && targetMeta == defaultMapMeta) {
			targetMeta = ((EngineImpl)engine).genericCollectionMeta;
			cacheIdx = targetMeta.cacheIndex;
		} else if (isArray && targetMeta != null && targetMeta.metaType != ObjectMeta.TYPE_OBJ_ARRAY && targetMeta.metaType != ObjectMeta.TYPE_COLLECTION && targetMeta.metaType != ObjectMeta.TYPE_SET) {
			throw new IllegalStateException("Expected object, got '['");
		}

		final var startTypeDesc = createTypeDesc(isArray, false, cacheIdx);
		final var startObj = targetMeta != null ? targetMeta.start(this) : null;
		final var startIdx = isArray ? 0 : -1;

		return runStateEngine(startTypeDesc, startObj, targetMeta, startIdx);
	}

	// ========================================================================
	// RECURSIVE FAST-PATH ENGINE (Mit Hybrid-Fallback zur State-Machine)
	// ========================================================================

	private double[] parseDoubleArray(double[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new double[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = parseDoublePrimitive();
			consumeCommaIfPresent();
		}
	}

	private int[] parseIntArray   (int[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new int[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = (int) parseLongPrimitive();
			consumeCommaIfPresent();
		}
	}

	private long[] parseLongArray(long[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new long[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = parseLongPrimitive();
			consumeCommaIfPresent();
		}
	}

	private boolean[] parseBooleanArray(boolean[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new boolean[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = parseBooleanPrimitive();
			consumeCommaIfPresent();
		}
	}

	private float[] parseFloatArray(float[] builder) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == builder.length) {
				final var newArr = new float[builder.length * 2];
				System.arraycopy(builder, 0, newArr, 0, builder.length);
				builder = newArr;
			}
			builder[idx++] = (float) parseDoublePrimitive();
			consumeCommaIfPresent();
		}
	}

	private Object parseDefaultArray(Object builder, final Class<?> compType) throws IOException {
		pos++;
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; lastArraySize = idx; return builder; }
			if (idx == Array.getLength(builder)) {
				final var newArr = java.lang.reflect.Array.newInstance(compType, idx * 2);
				System.arraycopy(builder, 0, newArr, 0, idx);
				builder = newArr;
			}
			Array.set(builder, idx++, parsePrimitiveInline(buffer[pos], compType));
			consumeCommaIfPresent();
		}
	}

	private Object parsePrimitiveArray(final int primId, final Class<?> fallbackCompType) throws IOException {
		final var startSize = (lastArraySize > 1 && lastArraySize < 64) ? lastArraySize : 16;
		return switch (primId) {
		case ObjectMeta.PRIM_DOUBLE  -> parseDoubleArray (new double [startSize]);
		case ObjectMeta.PRIM_LONG    -> parseLongArray   (new long   [startSize]);
		case ObjectMeta.PRIM_INT     -> parseIntArray    (new int    [startSize]);
		case ObjectMeta.PRIM_BOOLEAN -> parseBooleanArray(new boolean[startSize]);
		case ObjectMeta.PRIM_FLOAT   -> parseFloatArray  (new float  [startSize]);
		default -> {
			pos++;
			// Fallback für char/byte/short
			yield parseDefaultArray(Array.newInstance(fallbackCompType, startSize), fallbackCompType);
		}
		};

	}

	private Object parseRecordRecursive(final ObjectMeta meta, final int recursionDeep) throws Throwable {
		final var context = meta.start(this);
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) '}') { pos++; return meta.end(this, context); }
			expect((byte) '"');
			final var targetIdx = parseStringKeyAsIndex(context, meta);
			if (pos < limit && buffer[pos] == (byte) ':') pos++;
			else { skipWhitespace(); expect((byte) ':'); }
			if (targetIdx < 0) { skipWhitespace(); skipValue(); consumeCommaIfPresent(); continue; }

			final var type = meta.type(targetIdx);
			skipWhitespace();
			final Object value;
			final var b = buffer[pos];

			switch (b) {
			case '-','0','1','2','3','4','5','6','7','8','9' -> parseNumericPrimitive(meta, context, targetIdx, type);
			case 'n' -> fillNullValue(-1L, context, targetIdx, meta, type);
			case 't' -> meta.set(this, context, targetIdx, parseTruePrimitive());
			case 'f' -> meta.set(this, context, targetIdx, parseFalsePrimitive());
			case '"' -> meta.set(this, context, targetIdx, parseStringValue());
			case '{' -> {
				pos++;
				final var childDesc = meta.getChildDescriptor(targetIdx);

				if (childDesc < 0L) throw new IllegalStateException("Expected array, got '{'");
				if ((childDesc & 1L) != 0L) throw new IllegalStateException("Expected primitive, got '{'");

				final var childMeta = metaCache[(int) (childDesc >> 1)];
				if (recursionDeep >= this.maxDepth) {
					value = runStateEngine(childDesc, childMeta.start(this), childMeta, -1);
				} else {
					value = parseRecordRecursive(childMeta, recursionDeep + 1);
				}
				meta.set(this, context, targetIdx, value);
			}
			case '[' -> {
				pos++;
				var childDesc = meta.getChildDescriptor(targetIdx);

				// Dynamischer Switch von Object zu Collection, falls ein untypisiertes JSON-Array kommt
				if (childDesc >= 0L) {
					final var expectedMeta = metaCache[(int) (childDesc >> 1)];
					if (expectedMeta != defaultMapMeta) {
						throw new IllegalStateException("Expected object, got '['");
					}
					final var colIdx = ((EngineImpl)engine).genericCollectionMeta.cacheIndex;
					childDesc = createTypeDesc(true, false, colIdx);
				}

				// Bitmaske sagt uns: Ist es ein Primitiv-Array?
				if ((childDesc & 1L) != 0L) {
					final var primId = (int) ((childDesc & 0x7FFFFFFFFFFFFFFFL) >> 1);
					final var fallback = type != null ? type.componentType() : null;
					value = parsePrimitiveArray(primId, fallback);
				} else {
					final var childMeta = metaCache[(int) (childDesc >> 1)];
					if (recursionDeep >= this.maxDepth) {
						value = runStateEngine(childDesc, childMeta.start(this), childMeta, 0);
					} else {
						value = parseArrayRecursive(childDesc, childMeta, recursionDeep + 1);
						depth = recursionDeep; // Reset depth nach Rekursion
					}
				}
				meta.set(this, context, targetIdx, value);
			}
			default -> throw new IllegalStateException("Unexpected character: " + (char) b);
			}
			consumeCommaIfPresent();
		}
	}

	private Object parsePrimitiveInline(final byte b, final Class<?> type) throws IOException {
		return switch (b) {
		case '"' -> parseStringValue();
		case 't' -> parseTrue();
		case 'f' -> parseFalse();
		case 'n' -> parseNull();
		default  -> parseNumber(type);
		};
	}

	@Override public int depth() { return depth; }

	private Object parseArrayRecursive(final long typeDesc, final ObjectMeta meta, final int recursionDeep) throws Throwable {
		depth = recursionDeep;
		final var context = meta.start(this);
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == (byte) ']') { pos++; return meta.end(this, context); }

			final var b = buffer[pos];
			switch (b) {
			case '-','0','1','2','3','4','5','6','7','8','9' -> parseNumericPrimitive(meta, context, idx++, meta.type(0));
			case 'n' -> fillNullValue(0x8000000000000000L, context, idx++, meta, meta.type(0));
			case 't' -> meta.set(this, context, idx++, parseTruePrimitive());
			case 'f' -> meta.set(this, context, idx++, parseFalsePrimitive());
			case '"' -> meta.set(this, context, idx++, parseStringValue());
			case '{' -> {
				pos++;
				final Object val;
				final var childDesc = meta.getComponentDescriptor(); // Bei Arrays ist es immer getComponentDescriptor()
				final var childMeta = metaCache[(int) (childDesc >> 1)];

				if (recursionDeep >= this.maxDepth) {
					val = runStateEngine(childDesc, childMeta.start(this), childMeta, -1);
				} else {
					val = parseRecordRecursive(childMeta, recursionDeep + 1);
				}
				meta.set(this, context, idx++, val);
			}
			case '[' -> {
				pos++;
				final Object val;
				final var childDesc = meta.getComponentDescriptor();

				if ((childDesc & 1L) != 0L) {
					final var primId = (int) ((childDesc & 0x7FFFFFFFFFFFFFFFL) >> 1);
					val = parsePrimitiveArray(primId, meta.type(0).componentType());
				} else {
					final var childMeta = metaCache[(int) (childDesc >> 1)];
					if (recursionDeep >= this.maxDepth) {
						val = runStateEngine(childDesc, childMeta.start(this), childMeta, 0);
					} else {
						val = parseArrayRecursive(childDesc, childMeta, recursionDeep + 1);
						depth = recursionDeep;
					}
				}
				meta.set(this, context, idx++, val);
			}
			default -> throw new IllegalStateException("Unexpected character in array: " + (char) b);
			}
			consumeCommaIfPresent();
		}
	}
}