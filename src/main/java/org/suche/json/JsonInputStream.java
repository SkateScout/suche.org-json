package org.suche.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public final class JsonInputStream extends BufferedStream implements AutoCloseable, MetaPool {
	private static final ObjectPool<JsonInputStream> STREAM_POOL = new ObjectPool<>(64, JsonInputStream::new);

	public ObjectMeta[] metaCache;

	static final class CTX {
		long typeDesc;
		Object obj;
		Object meta;
		int targetIdx;
		void set(final long pTypeDesc, final Object pObj, final Object pMeta, final int pTargetIdx) { this.typeDesc = pTypeDesc; this.obj = pObj; this.meta = pMeta; this.targetIdx = pTargetIdx; }
		public void clean() { this.obj = null; this.meta = null; }
	}

	static final class CtxStack {
		private final CTX[] stack64 = new CTX[64];
		CTX[] stack = stack64;
		int depth = -1;
		CtxStack() { for (var i = 0; i < 64; i++) stack[i] = new CTX(); }
		void push(final long typeDesc, final Object obj, final Object meta, final int targetIdx) {
			if (++depth >= stack.length) {
				final var oldLen = stack.length;
				stack = Arrays.copyOf(stack, oldLen * 2);
				for (var i = oldLen; i < stack.length; i++) stack[i] = new CTX();
			}
			stack[depth].set(typeDesc, obj, meta, targetIdx);
		}
		void clear() { for (var i = 0; i <= depth; i++) stack[i].clean(); depth = -1; }
	}

	private final CtxStack engineStack = new CtxStack();
	private int lastArraySize = 16;

	private JsonInputStream() {}

	static JsonInputStream of(final InputStream in, final InternalEngine engine) {
		final var s = STREAM_POOL.acquire();
		s.init(in, engine);
		s.engineStack.depth = -1;
		s.lastArraySize = 16;
		s.metaCache = engine.metaCache();
		return s;
	}

	@Override
	public void close() throws IOException {
		engineStack.clear();
		depth = -1;
		if (internPool != null) internPool.clear();
		Arrays.fill(onceInternal, 0, onceInternalSize, null);
		onceInternalSize = 0;
		final var oldIn = this.in;
		this.in = null;
		STREAM_POOL.release(this);
		if (oldIn != null) oldIn.close();
	}

	private void fillNullValue(final long typeDesc, final Object obj, final int targetIdx, final ObjectMeta meta) throws IOException {
		parseNull();
		if (engine.config().skipDefaultValues() && typeDesc >= 0L) return;
		// Extract primitive ID from descriptor
		final var primId = (int) (typeDesc >> 1);	// No mask since only highest bit is used as marker for array/object
		switch (primId) {
		case ObjectMeta.PRIM_BOOLEAN -> meta.set(this, obj, targetIdx, Boolean.FALSE);
		case ObjectMeta.PRIM_DOUBLE, ObjectMeta.PRIM_FLOAT -> meta.setDouble(this, obj, targetIdx, 0.0);
		case ObjectMeta.PRIM_INT, ObjectMeta.PRIM_LONG -> meta.setLong(this, obj, targetIdx, 0L);
		default -> meta.set(this, obj, targetIdx, null);
		}
	}

	private Object runStateEngine(final long startTypeDesc, final Object startObj, final Object startMeta, final int startIdx) throws Throwable {
		var curTypeDesc = startTypeDesc;
		var curObj = startObj;
		var curMeta = (ObjectMeta) startMeta;
		var curIdx = startIdx;
		engineStack.clear();
		while (true) {
			if (pos >= limit) { ensure(1); if (pos >= limit) throw new IllegalStateException(); }
			var b = buffer[pos];
			switch (b) {
			case '\t', '\n', '\r', ' ' -> {
				for(pos++; pos < limit; pos++) if (((b = buffer[pos]) & 0xC0) != 0 || ((1L << b) & WHITESPACE_MASK) == 0) break;
			}
			case '"' -> {
				if (curTypeDesc >= 0L && curIdx < 0) {
					pos++;
					curIdx = curMeta.prepareKey(curObj, parseStringValue());
					expect((byte) ':');
				} else {
					curMeta.set(this, curObj, curIdx, parseStringValue());
					curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
					consumeCommaIfPresent();
				}
			}
			case '}', ']' -> {
				pos++;
				final var finishedObj = curMeta.end(this, curObj);
				if (engineStack.depth < 0) return finishedObj;
				final var parent = engineStack.stack[engineStack.depth--];
				curTypeDesc = parent.typeDesc;
				curObj = parent.obj;
				curMeta = (ObjectMeta) parent.meta;
				curIdx = parent.targetIdx;
				curMeta.set(this, curObj, curIdx, finishedObj);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> {
				parseNumericPrimitive(curMeta, curObj, curIdx, null);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case 'n' -> {
				fillNullValue(curTypeDesc, curObj, curIdx, curMeta);
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case 't', 'f' -> {
				curMeta.set(this, curObj, curIdx, b == 't' ? parseTruePrimitive() : parseFalsePrimitive());
				curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
				consumeCommaIfPresent();
			}
			case '{' -> {
				engineStack.push(curTypeDesc, curObj, curMeta, curIdx);	// No issue if later an exception came
				if (curTypeDesc >= 0L && curIdx < 0) throw new IllegalStateException("Expected key");
				curTypeDesc = curMeta.fieldDescriptor(curIdx);
				pos++;
				if (curTypeDesc < 0L || (curTypeDesc & 1L) != 0L) throw new IllegalStateException("Expected got unexpected '{'");
				curMeta = metaCache[(int) (curTypeDesc >> 1)];
				curObj = curMeta.start(this);
				curIdx = -1;
			}
			case '[' -> {
				if (curTypeDesc >= 0L && curIdx < 0) throw new IllegalStateException("Expected key");
				pos++;
				var childDesc = curMeta.fieldDescriptor(curIdx);
				if (childDesc >= 0L) {
					if (((int) (childDesc >> 1))!= ObjectMeta.IDX_MAP) throw new IllegalStateException("Expected object, got '['");
					// TODO this should be an constant. not an method call here
					childDesc = EngineImpl.createTypeDesc(true, false, ObjectMeta.IDX_COLLECTION);
				}
				if ((childDesc & 1L) != 0L) {
					// TODO remove bit shift here and move it to parsePrimitiv
					curMeta.set(this, curObj, curIdx, parsePrimitiveArray((int) (childDesc >> 1))); // cast to int avoid mask with & 0x7FFFFFFFFFFFFFFFL
					curIdx = curTypeDesc < 0L ? curIdx + 1 : -1;
					consumeCommaIfPresent();
				} else {
					engineStack.push(curTypeDesc, curObj, curMeta, curIdx);
					curTypeDesc = childDesc;
					curMeta = metaCache[(int) (curTypeDesc >> 1)];
					curObj = curMeta.start(this);
					curIdx = 0;
				}
			}
			default -> throw new IllegalStateException();
			}
		}
	}

	private void consumeCommaIfPresent() throws IOException {
		if (pos < limit && buffer[pos] == (byte) ',') { pos++; return; }
		skipWhitespace();
		if (pos < limit && buffer[pos] == (byte) ',') pos++;
	}

	public Object readObject(final Class<?> targetType) throws Throwable {
		skipWhitespace();
		if (pos >= limit) return null;
		final var b = buffer[pos++];
		final var targetMeta = engine.metaOf(targetType);
		final var isArray = (b == '[');
		final var startTypeDesc = EngineImpl.createTypeDesc(isArray, false, targetMeta != null ? targetMeta.cacheIndex : 0);
		return runStateEngine(startTypeDesc, targetMeta != null ? targetMeta.start(this) : null, targetMeta, isArray ? 0 : -1);
	}

	private Object parsePrimitiveArray(final int primId) throws IOException {
		final var size = (lastArraySize > 0 && lastArraySize < 1024) ? lastArraySize : 16;
		return switch (primId) {
		case ObjectMeta.PRIM_DOUBLE  -> parseDoubleArray(new double[size]);
		case ObjectMeta.PRIM_LONG    -> parseLongArray(new long[size]);
		case ObjectMeta.PRIM_INT     -> parseIntArray(new int[size]);
		case ObjectMeta.PRIM_BOOLEAN -> parseBooleanArray(new boolean[size]);
		default -> null; // Fallback für char/byte/short
		};
	}

	private double[] parseDoubleArray(double[] b) throws IOException {
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == ']') { pos++; lastArraySize = idx; return Arrays.copyOf(b, idx); }
			if (idx == b.length) b = Arrays.copyOf(b, b.length << 1);
			b[idx++] = parseDoublePrimitive();
			consumeCommaIfPresent();
		}
	}

	private int[] parseIntArray(int[] b) throws IOException {
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == ']') { pos++; lastArraySize = idx; return Arrays.copyOf(b, idx); }
			if (idx == b.length) b = Arrays.copyOf(b, b.length << 1);
			b[idx++] = (int) parseLongPrimitive();
			consumeCommaIfPresent();
		}
	}

	private long[] parseLongArray(long[] b) throws IOException {
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == ']') { pos++; lastArraySize = idx; return Arrays.copyOf(b, idx); }
			if (idx == b.length) b = Arrays.copyOf(b, b.length << 1);
			b[idx++] = parseLongPrimitive();
			consumeCommaIfPresent();
		}
	}

	private boolean[] parseBooleanArray(boolean[] b) throws IOException {
		var idx = 0;
		while (true) {
			skipWhitespace();
			if (pos < limit && buffer[pos] == ']') { pos++; lastArraySize = idx; return Arrays.copyOf(b, idx); }
			if (idx == b.length) b = Arrays.copyOf(b, b.length << 1);
			b[idx++] = parseBooleanPrimitive();
			consumeCommaIfPresent();
		}
	}

	@Override public int depth() { return engineStack.depth; }
}