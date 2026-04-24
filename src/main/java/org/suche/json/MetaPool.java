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
				for (var i = 0; i < cnt; i++) objs[i] = org.suche.json.CompactMap.PRIMITIVE.LONG;
			} else if (singleType == MetaPool.T_DOUBLE) {
				for (var i = 0; i < cnt; i++) objs[i] = org.suche.json.CompactMap.PRIMITIVE.DOUBLE;
			}
			singleType = MetaPool.T_MIXED;
		}

		void ensureObjs(final MetaPool s, final int minCap) {
			final var cur = objs == null ? 0 : objs.length;
			if (cur >= minCap) return;
			var newCap = cur == 0 ? minCap : cur << 1;
			if (newCap < minCap) newCap = minCap;
			final var arr = s.takeArray(newCap);
			if (objs != null) {
				final var copyLen = Math.min(cnt, objs.length);
				if (copyLen > 0) System.arraycopy(objs, 0, arr, 0, copyLen);
				s.returnArray(objs, copyLen);
			}
			objs = arr;
		}

		void ensurePrims(final MetaPool s, final int minCap) {
			final var cur = prims == null ? 0 : prims.length;
			if (cur >= minCap) return;
			var newCap = cur == 0 ? minCap : cur << 1;
			if (newCap < minCap) newCap = minCap;
			final var arr = s.takeLongArray(newCap);
			if (prims != null) {
				final var pCnt = cnt >> map;
			final var copyLen = Math.min(pCnt, prims.length);
			if (copyLen > 0) System.arraycopy(prims, 0, arr, 0, copyLen);
			s.returnLongArray(prims);
			}
			prims = arr;
		}
	}

	Object[]     takeArray    (int minCapacity);
	long[]       takeLongArray(int minCapacity);
	ParseContext takeContext  (int minCapacity, boolean map);

	void         returnArray    (Object[] arr, int used);
	void         returnLongArray(long  [] arr);
	void         returnContext  (ParseContext ctx);

	int          depth();

	Object deduplicate(final Object value);

	String internBytes(byte[] src, int start, int len, int hash, boolean isAscii);

	InternalEngine engine();
}