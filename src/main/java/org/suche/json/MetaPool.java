package org.suche.json;

interface MetaPool {
	byte T_EMPTY  = 0;
	byte T_LONG   = 1;
	byte T_DOUBLE = 2;
	byte T_MIXED  = 3;

	static final class ParseContext {
		int      map  = 0;
		Object[] objs;
		long[]   prims;
		byte     singleType;
		int      cnt;
		String   currentKey;

		void upgradeToDouble() {
			if (singleType != MetaPool.T_LONG) return;
			for (var i = 0; i < cnt; i++) {
				prims[i] = Double.doubleToRawLongBits(prims[i]);
			}
			singleType = MetaPool.T_DOUBLE;
		}

		void upgradeToMixed(final MetaPool s, final int requiredCapacity) {
			if (singleType == MetaPool.T_MIXED) return;
			if (objs == null) objs = s.takeArray(requiredCapacity);

			if (singleType == MetaPool.T_LONG) {
				for (var i = 0; i < cnt; i++) objs[i] = org.suche.json.PRIMITIVE.LONG;
			} else if (singleType == MetaPool.T_DOUBLE) {
				for (var i = 0; i < cnt; i++) objs[i] = org.suche.json.PRIMITIVE.DOUBLE;
			}
			singleType = MetaPool.T_MIXED;
		}

		void ensureObjs(final MetaPool s, final int minCapacity) {
			if (objs == null) {
				objs = s.takeArray(minCapacity);
			} else if (minCapacity > objs.length) {
				var newCap = objs.length << 1;
				if (newCap < minCapacity) newCap = minCapacity;
				final var newObjs = s.takeArray(newCap);
				System.arraycopy(objs, 0, newObjs, 0, objs.length);
				s.returnArray(objs, objs.length);
				objs = newObjs;
			}
		}

		void ensurePrims(final MetaPool s, final int minCapacity) {
			if (prims == null) {
				prims = s.takeLongArray(minCapacity);
			} else if (minCapacity > prims.length) {
				var newCap = prims.length << 1;
				if (newCap < minCapacity) newCap = minCapacity;
				final var newPrims = s.takeLongArray(newCap);
				System.arraycopy(prims, 0, newPrims, 0, prims.length);
				s.returnLongArray(prims);
				prims = newPrims;
			}
		}

		void primKeyValue(final MetaPool s, final PRIMITIVE type, final long value) {
			final var valIdx = (this.cnt >> 1);
			if (this.objs == null || this.cnt + 2 > this.objs.length) this.ensureObjs(s, this.cnt + 2);
			if (this.prims == null || valIdx >= this.prims.length) this.ensurePrims(s, valIdx + 1);
			this.objs[this.cnt++] = this.currentKey;
			this.objs[this.cnt++] = type;
			this.prims[valIdx] = value;
			this.currentKey = null;
		}

		void primIdxValue(final MetaPool s, final PRIMITIVE t, final byte type, final long value, final int index) {
			if (this.singleType == MetaPool.T_EMPTY) this.singleType = type;
			else if (this.singleType == MetaPool.T_LONG && type == MetaPool.T_DOUBLE) this.upgradeToDouble();

			if (this.singleType == MetaPool.T_MIXED) {
				if (this.objs == null || index >= this.objs.length) this.ensureObjs(s, index + 1);
				if (this.prims == null || index >= this.prims.length) this.ensurePrims(s, index + 1);
				this.objs[index] = t;
				this.prims[index] = value;
			} else {
				if (this.prims == null || index >= this.prims.length) this.ensurePrims(s, index + 1);
				this.prims[index] = (this.singleType == MetaPool.T_DOUBLE && type == MetaPool.T_LONG) ? Double.doubleToRawLongBits(value) : value;
			}
			if (index >= this.cnt) this.cnt = index + 1;
		}


	}

	Object[]     takeArray    (int minCapacity);
	long[]       takeLongArray(int minCapacity);
	ParseContext takeContext  (boolean map);

	void         returnArray    (Object[] arr, int used);
	void         returnLongArray(long  [] arr);
	void         returnContext  (ParseContext ctx);

	int          depth();

	Object deduplicate(final Object value);

	String internBytes(byte[] src, int start, int len, int hash, boolean isAscii);

	InternalEngine engine();
}