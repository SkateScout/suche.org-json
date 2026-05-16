package org.suche.json;

final class PRIMITIVE extends Number {
	static final byte T_EMPTY  = 0;
	static final byte T_LONG   = 1;
	static final byte T_DOUBLE = 2;
	static final byte T_MIXED  = 3;

	public static final PRIMITIVE EMPTY   = new PRIMITIVE(T_EMPTY );
	public static final PRIMITIVE MIXED   = new PRIMITIVE(T_LONG  );
	public static final PRIMITIVE LONG    = new PRIMITIVE(T_DOUBLE);
	public static final PRIMITIVE DOUBLE  = new PRIMITIVE(T_MIXED );

	private static final long serialVersionUID = 1L;
	private PRIMITIVE(final byte pType) { this.type = pType; }
	final byte type;
	@Override public int    intValue   () { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public long   longValue  () { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public float  floatValue () { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public double doubleValue() { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
}