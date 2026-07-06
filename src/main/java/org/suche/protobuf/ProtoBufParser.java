package org.suche.protobuf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.suche.protobuf.ObjectMeta.FieldInfo;

public class ProtoBufParser {

	private static final IllegalArgumentException error(final String mesg) {
		throw new IllegalArgumentException(mesg);
	}

	private static final IllegalArgumentException intError(final String mesg, final int val) {
		throw new IllegalArgumentException(mesg.replace("{val}", Integer.toString(val)));
	}

	private static final IllegalArgumentException error(final String mesg, final Class<?> cls, final FieldInfo info) {
		throw new IllegalArgumentException(mesg.replace("{cls}", cls.getCanonicalName()).replace("{name}", info.keyName()));
	}

	public static int    readLength (final ByteBuffer buffer) {
		final var length = readVarInt(buffer);
		if (length < 0 || buffer.remaining() < length) throw intError("Invalid Protobuf length ({val}) or buffer underflow.", length);
		return length;
	}

	public static int    readVarInt (final ByteBuffer buffer) {
		var result = 0;
		var shift = 0;
		while (shift < 32) {
			if (!buffer.hasRemaining()) throw error("Buffer underflow while reading varint");
			final var b = buffer.get();
			result |= (b & 0x7F) << shift;
			if ((b & 0x80) == 0) return result;
			shift += 7;
		}
		throw error("Malformed varint (too many bytes for 32-bit integer)");
	}

	public static void   skipGroup  (final ByteBuffer buffer) {
		while (buffer.hasRemaining()) {
			final var tag = readVarInt(buffer);
			final var wireType = tag & 0x07;
			if (wireType == 4) return; // Passendes End-Group Tag gefunden, Abbruch der Schleife
			skipField(buffer, wireType); // Rekursiv weiter überspringen
		}
		throw error("Unterbrochener Stream: End-group Tag (Wire Type 4) fehlt.");
	}

	public static void   skipField  (final ByteBuffer buffer, final int wireType) {
		switch (wireType) {
		case  0 -> readVarInt(buffer);                     // Varint (int32, int64, uint32, bool, enum, ...)
		case  1 -> buffer.position(buffer.position() + 8); // 64-bit (double, fixed64, sfixed64, ...)
		case  2 -> buffer.position(buffer.position() + readLength(buffer)); // Length-delimited (string, bytes, embedded messages, packed repeated fields)
		case  3 -> skipGroup(buffer); // "Start group (Deprecated sinced P2) replaced with Embedded Messages & Wire Type 2 ersetzt"
		case  4 -> throw error("End group (Deprecated sinced P2) replaced with Embedded Messages & Wire Type 2 ersetzt");
		case  5 -> buffer.position(buffer.position() + 4); // 32-bit (float, fixed32, ...)
		case  6 -> throw error("Reserved");
		case  7 -> throw error("Reserved");
		default -> throw intError("Unsupported or corrupt wire type: {val}", wireType);
		}
	}

	public static String readString (final ByteBuffer buffer, final int length) {
		final var ret = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), length, StandardCharsets.UTF_8);
		buffer.position(buffer.position() + length);
		return ret;
	}

	public static long   readVarLong(final ByteBuffer buffer) {
		var result = 0L;
		var shift = 0;
		while (shift < 64) {
			if (!buffer.hasRemaining()) throw error("Buffer underflow while reading varlong");
			final var b = buffer.get();
			result |= (long)(b & 0x7F) << shift;
			if ((b & 0x80) == 0) return result;
			shift += 7;
		}
		throw error("Malformed varint (too many bytes for 64-bit integer)");
	}

	public static float [] readPackedFloats (final ByteBuffer buffer, final int length) {
		if (length % 4 != 0) throw intError("Malformed packed float array: length ({val}) is not a multiple of 4.", length);
		final var count = length / 4;
		final var floats = new float[count];
		for (var i = 0; i < count; i++) floats[i] = buffer.getFloat();
		return floats;
	}

	public static double[] readPackedDoubles(final ByteBuffer buffer, final int length) {
		if (length % 8 != 0) throw intError("Malformed packed double array: length (" + length + ") is not a multiple of 8.", length);
		final var count = length / 8;
		final var doubles = new double[count];
		for (var i = 0; i < count; i++) doubles[i] = buffer.getDouble();
		return doubles;
	}

	public static int   [] readPackedInts   (final ByteBuffer buffer, final int length) {
		final var end = buffer.position() + length;
		var temp = new int[length / 2 + 1]; // Gute Schätzung für VarInt-Größe
		var count = 0;
		while (buffer.position() < end) {
			if (count == temp.length) temp = java.util.Arrays.copyOf(temp, count * 2);
			temp[count++] = readVarInt(buffer);
		}
		return java.util.Arrays.copyOf(temp, count);
	}

	public static long  [] readPackedLongs  (final ByteBuffer buffer, final int length) {
		final var end = buffer.position() + length;
		var temp = new long[length / 2 + 1];
		var count = 0;
		while (buffer.position() < end) {
			if (count == temp.length) temp = java.util.Arrays.copyOf(temp, count * 2);
			temp[count++] = readVarLong(buffer);
		}
		return java.util.Arrays.copyOf(temp, count);
	}

	public static <T extends Record> T decode(final byte[] protobufBytes, final Class<T> recordClass) {
		return decode(ByteBuffer.wrap(protobufBytes).order(ByteOrder.LITTLE_ENDIAN), recordClass);
	}

	@SuppressWarnings("unchecked")
	public static <T extends Record> T decode(final ByteBuffer buf, final Class<T> recordClass) {
		final ObjectMeta<T> meta = ObjectMeta.of(recordClass);
		final var clen = meta.fieldSize();
		final var objects = new Object[clen];
		final var prims   = new long  [clen];
		while (buf.hasRemaining()) {
			final var tag      = readVarInt(buf);
			final var fieldId  = (tag >>> 3);
			final var wireType = tag & 0x07;
			final var info     = meta.field(fieldId);
			if (info == null) { meta.skip(fieldId); skipField(buf, wireType); continue; }
			final var cpos = info.cpos();
			if(objects[cpos] != null && objects[cpos].getClass().isArray()) throw error("Array concat not supported yet for class {cls} field {name}", recordClass, info);
			switch(info.classIndex()) {
			case ObjectMeta.T_STRING     -> addObj(objects, prims, info, readString(buf, readLength(buf)), 0);
			case ObjectMeta.T_FLOAT_ARR  -> objects[cpos] = readPackedFloats (buf, readLength(buf));
			case ObjectMeta.T_DOUBLE_ARR -> objects[cpos] = readPackedDoubles(buf, readLength(buf));
			case ObjectMeta.T_INT_ARR    -> objects[cpos] = readPackedInts   (buf, readLength(buf));
			case ObjectMeta.T_LONG_ARR   -> objects[cpos] = readPackedLongs  (buf, readLength(buf));
			case ObjectMeta.T_INT        -> {
				if (wireType == 2) { // Packed Array, das als List<Integer> in das Record soll
					final var ints = readPackedInts(buf, readLength(buf));
					if (info.isRepeated()) {
						if (objects[cpos] == null) objects[cpos] = new ArrayList<Integer>();
						final var list = (List<Object>) objects[cpos];
						for (final var v : ints) list.add(v);
					}
				} else { // Reguläres VarInt (Wire Type 0)
					final var val = readVarInt(buf);
					addObj(objects, prims, info, val, val);
				}
			}
			case ObjectMeta.T_LONG       -> {
				if (wireType == 2) { // Packed Array -> List<Long>
					final var longs = readPackedLongs(buf, readLength(buf));
					if (info.isRepeated()) {
						if (objects[cpos] == null) objects[cpos] = new ArrayList<Long>();
						final var list = (List<Object>) objects[cpos];
						for (final var v : longs) list.add(v);
					}
				} else { // Reguläres VarLong (Wire Type 0)
					final var val = readVarLong(buf);
					addObj(objects, prims, info, val, val);
				}
			}
			case ObjectMeta.T_FLOAT      -> {
				if (wireType == 2) { // Packed Array -> List<Float>
					final var floats = readPackedFloats(buf, readLength(buf));
					if (info.isRepeated()) {
						if (objects[cpos] == null) objects[cpos] = new ArrayList<Float>();
						final var list = (List<Object>) objects[cpos];
						for (final var v : floats) list.add(v);
					}
				} else { // 32-Bit Float (Wire Type 5)
					final var val = buf.getFloat();
					addObj(objects, prims, info, val, Float.floatToRawIntBits(val));
				}
			}
			case ObjectMeta.T_DOUBLE     -> {
				if (wireType == 2) { // Packed Array -> List<Double>
					final var doubles = readPackedDoubles(buf, readLength(buf));
					if (info.isRepeated()) {
						if (objects[cpos] == null) objects[cpos] = new ArrayList<Double>();
						final var list = (List<Object>) objects[cpos];
						for (final var v : doubles) list.add(v);
					}
				} else { // 64-Bit Double (Wire Type 1)
					final var val = buf.getDouble();
					addObj(objects, prims, info, val, Double.doubleToRawLongBits(val));
				}
			}
			case ObjectMeta.T_RECORD     -> { // 4. VERSCHACHTELTE RECORDS
				final var length = readLength(buf);
				final var subBuffer = buf.slice().limit(length).order(ByteOrder.LITTLE_ENDIAN);
				addObj(objects, prims, info, decode(subBuffer, (Class<? extends Record>) info.effCls()), 0);
				buf.position(buf.position() + length);
			}
			default                      -> skipField(buf, wireType);
			}
		}
		return (T) meta.create(objects, prims);
	}

	@SuppressWarnings("unchecked")
	public static void addObj(final Object[] objects, final long[] prims, final FieldInfo info, final Object item, final long longVal) {
		final var cpos = info.cpos();
		if (info.isRepeated()) {
			if (objects[cpos] == null) objects[cpos] = new ArrayList<>();
			((List<Object>) objects[cpos]).add(item);
		} else {
			objects[cpos] = item;
			prims  [cpos] = longVal;
		}
	}
}