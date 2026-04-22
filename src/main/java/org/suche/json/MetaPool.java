package org.suche.json;

interface MetaPool {
	static final class ParseContext {
		Object[] objs;
		long[]   prims;
		int      cnt;
		String   currentKey;
	}

	Object[]     takeArray(int minCapacity);
	long[]       takeLongArray(int minCapacity);
	ParseContext takeContext(int minCapacity);

	void         returnArray(Object[] arr);
	void         returnLongArray(long[] arr);
	void         returnContext(ParseContext ctx);

	Object deduplicate(final Object value);

	String internBytes(byte[] src, int start, int len, int hash, boolean isAscii);
	InternalEngine engine();
}