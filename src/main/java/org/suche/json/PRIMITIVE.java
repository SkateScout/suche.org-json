package org.suche.json;

final class PRIMITIVE extends Number {
	static final byte T_EMPTY  = 0;
	static final byte T_LONG   = 1;
	static final byte T_DOUBLE = 2;
	static final byte T_MIXED  = 3;

	public static final PRIMITIVE EMPTY   = new PRIMITIVE("EMPTY" , T_EMPTY );
	public static final PRIMITIVE MIXED   = new PRIMITIVE("MIXED" , T_MIXED );
	public static final PRIMITIVE LONG    = new PRIMITIVE("LONG"  , T_LONG  );
	public static final PRIMITIVE DOUBLE  = new PRIMITIVE("DOUBLE", T_DOUBLE);

	private static final long serialVersionUID = 1L;
	private PRIMITIVE(final String pName, final byte pType) { this.name = pName; this.type = pType; }
	final byte   type;
	final String name;
	@Override public int    intValue   () { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public long   longValue  () { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public float  floatValue () { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public double doubleValue() { throw JsonEngine.illegalStateException("Unresolved lazy primitive"); }
	@Override public String toString   () { return name; }
}