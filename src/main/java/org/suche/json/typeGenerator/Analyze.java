package org.suche.json.typeGenerator;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.suche.json.JsonEngine;
import org.suche.json.MetaConfig;


public class Analyze {
	public static final class PropertyInfo {
		static final int T_BYTE    = 1 <<  0; // Byte .MIN_VALUE   <= v <= Byte.MAX_VALUE
		static final int T_SHORT   = 1 <<  1; // Short.MIN_VALUE   <= v <= Short.MAX_VALUE
		static final int T_INT     = 1 <<  2; // Integer.MIN_VALUE <= v <= Integer.MAX_VALUE
		static final int T_LONG552 = 1 <<  3; // long may double without loose
		static final int T_LONG    = 1 <<  4; // Long.MIN_VALUE <= v <= Byte.MAX_VALUE
		static final int T_FLOAT   = 1 <<  5; // Unused
		static final int T_DOUBLE  = 1 <<  6;
		static final int T_CHAR    = 1 <<  7;
		static final int T_BOOLEAN = 1 <<  8;
		static final int T_STRING  = 1 <<  9;
		static final int T_ENUM    = 1 << 10;
		static final int T_ARRAY   = 1 << 11; // No difference between []/CollectionS
		static final int T_OBJECT  = 1 << 13;
		static final int T_NULL    = 1 << 14; // Explizit null used
		static final int T_MAP     = 1 << 15;

		static final String CLASS_KEY = "__class__";
		static final String ENUM_KEY  = "__enum__";
		static final String VALUE_KEY  = "value";
		private static final long LONG552_MAX_VALUE = 1<<62;
		private static final long LONG552_MIN_VALUE = -LONG552_MAX_VALUE;

		public int         types      = 0;
		public boolean     hasDefault = false; // 0 or false
		public int         useCount;   // Statistik only
		public int         size       = 0;
		public Set<String> classes    ;	// If not Primitive "__class__" names seen
		public Set<String> enums      ;	// Enumeration Values seen
		public Set<String> enumClasses;	// Enumeration Values seen

		// Return if to dive info
		boolean register(final Object v) {
			useCount++;
			return switch(v) {
			case null -> { types |= T_NULL; yield false; }
			case final Number        t -> {
				final var d = t.doubleValue();
				final var l = t.longValue  ();
				if(0 == d) hasDefault = true;
				if(((long)d) != l                                 ) { types |= T_DOUBLE ; yield false; }
				if(Byte   .MIN_VALUE <= l&& l <= Byte   .MAX_VALUE) { types |= T_BYTE   ; yield false; }
				if(Short  .MIN_VALUE <= l&& l <= Short  .MAX_VALUE) { types |= T_SHORT  ; yield false; }
				if(Integer.MIN_VALUE <= l&& l <= Integer.MAX_VALUE) { types |= T_INT    ; yield false; }
				if(LONG552_MIN_VALUE <= l&& l <= LONG552_MAX_VALUE) { types |= T_LONG552; yield false; }


				types |= T_LONG;
				yield false;
			}
			case final Boolean       t -> { types |= T_BOOLEAN; if(!t.booleanValue()) hasDefault = true;  yield false; }
			case final String        t -> { types |= T_STRING ; if(t.isEmpty      ()) hasDefault = true;  yield false; }
			case final Character     _ -> { types |= T_CHAR   ; yield false; }
			case final Collection<?> t -> { types |= T_ARRAY  ; size = Math.max(size,t.size()); if(t.isEmpty      ()) hasDefault = true; yield true; }
			case final Enum<?>       t -> { types |= T_ENUM   ; if(enums == null) enums = new HashSet<>(); enums.add(t.name()); yield false;}
			case final Map<?,?>      m -> {
				@SuppressWarnings("unchecked") final var t = (Map<String,Object>)m;
				size = Math.max(size,t.size());
				if(t.get(CLASS_KEY) instanceof final String cls) {
					if(classes == null) classes = new HashSet<>();
					classes.add(cls);
					types |= T_OBJECT;
					yield true;
				}
				if(t.get(ENUM_KEY) instanceof final String cls) {
					if(enumClasses == null) enumClasses = new HashSet<>();
					enumClasses.add(cls);
					types |= T_ENUM;
					if(t.get(VALUE_KEY) instanceof final String e) {
						(enums == null ? enums = new HashSet<>() : enums) .add(e);
					} else {
						System.err.println("ENUM-VALUE ? "+v);
					}
					yield false;
				}
				types |= T_MAP;
				yield true;
			}
			default -> {
				if(v.getClass().isArray()) { types |= T_ARRAY; yield true; }
				System.err.println("TYP- ? "+v.getClass().getCanonicalName()+" "+v);
				yield false;
			}
			};
		}

		// mayAbsent = this.useCount < useCount (in the opbject)
		public String resolveJavaType(final int maxUseCount) {
			final var t = this.types;
			final var mayAbsent = useCount < maxUseCount;
			// Wenn wir explizit 'null' gesehen haben oder das Feld in einigen Objekten fehlte
			final var nullable = (t & PropertyInfo.T_NULL) != 0 || (this.hasDefault && mayAbsent);
			// 1. Komplexe Typen
			if ((t & PropertyInfo.T_OBJECT) != 0) return this.classes.iterator().next(); // Nutzt den __class__ Namen
			if ((t & PropertyInfo.T_MAP   ) != 0) return "Map<String, Object>"; // TODO: Inneren Typ vom Child-Knoten holen
			if ((t & PropertyInfo.T_ARRAY ) != 0) return "List<Object>";        // TODO: Inneren Typ vom Array-Child holen
			if ((t & PropertyInfo.T_ENUM  ) != 0) return this.enumClasses != null ? this.enumClasses.iterator().next() : "String";

			// 2. Primitive Widening (Die Magie deiner Bitmaske)
			// Wir isolieren alle Bits, die Zahlen repräsentieren
			final var numerics = t & (PropertyInfo.T_BYTE | PropertyInfo.T_SHORT | PropertyInfo.T_INT | PropertyInfo.T_LONG | PropertyInfo.T_FLOAT | PropertyInfo.T_DOUBLE);

			if (numerics != 0) {
				// Das höchste gesetzte Bit gewinnt! (z.B. wenn BYTE und INT gesetzt sind, liefert das INT)
				final var highest = Integer.highestOneBit(numerics);
				switch (highest) {
				case PropertyInfo.T_DOUBLE: return nullable ? "Double"  : "double";
				case PropertyInfo.T_FLOAT : return nullable ? "Float"   : "float";
				case PropertyInfo.T_LONG  : return nullable ? "Long"    : "long";
				case PropertyInfo.T_INT   : return nullable ? "Integer" : "int";
				case PropertyInfo.T_SHORT : return nullable ? "Short"   : "short";
				case PropertyInfo.T_BYTE  : return nullable ? "Byte"    : "byte";
				default                   : break;
				}
			}

			// 3. Andere Primitiven und Fallbacks
			if ((t & PropertyInfo.T_BOOLEAN) != 0) return nullable ? "Boolean"   : "boolean";
			if ((t & PropertyInfo.T_STRING) != 0)  return "String";
			if ((t & PropertyInfo.T_CHAR) != 0)    return nullable ? "Character" : "char";

			return "Object"; // Fallback, falls Typ unklar ist (z.B. leere Liste)
		}
	}

	public static void printJavaRecords(final Map<String, ObjectInfo> objectInfo) {
		for (final var entry : objectInfo.entrySet()) {
			final var oi = entry.getValue();
			// Überspringe generische Maps und leere Objekte
			if (oi.className == null || "<MAP>".equals(oi.className)) continue;

			System.out.println("public record " + oi.className + "(");
			if (oi.properties != null) {

				var maxUseCount = 0;
				for (final var p : oi.properties.values()) if(p.useCount > maxUseCount) maxUseCount = p.useCount;

				var first = true;
				for (final var prop : oi.properties.entrySet()) {
					if (!first) System.out.println(",");
					first = false;
					System.out.print("    " + prop.getValue().resolveJavaType(maxUseCount) + " " + prop.getKey());
				}
				System.out.println();
			}
			System.out.println(") {}\n");
		}
	}

	public static final class ObjectInfo {
		public String                    className;
		public Map<String, PropertyInfo> properties;
	}

	public static Map<String,ObjectInfo> typeInfo(final Object root) {
		final var objectInfo   = new HashMap<String,ObjectInfo>();
		final var todo = new ArrayDeque<Map.Entry<String, Object>>();
		final var once = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
		todo.add(Map.entry("", root));
		while(!todo.isEmpty()) {
			final var stack = todo.remove();
			final var parentPath = stack.getKey();
			final var v = stack.getValue();
			if(!once.add(v)) continue;
			switch(v) {
			case final Map<?,?> m -> {
				@SuppressWarnings("unchecked") final var t = (Map<String,Object>)m;
				var name = "<MAP>";
				if      (t.get(PropertyInfo.CLASS_KEY) instanceof final String s) name = s;
				else  if(t.get(PropertyInfo.ENUM_KEY ) instanceof final String s) name = s;
				final var path = parentPath + "/" + name;
				final var oi = objectInfo.computeIfAbsent(path, _->new ObjectInfo());
				for(final var prop : t.entrySet()) {
					if(oi.properties == null) oi.properties = new HashMap<>();
					final var val = prop.getValue();
					final var p = prop.getKey();
					if(oi.properties.computeIfAbsent(path+"/"+p, _->new PropertyInfo()).register(val))
						todo.add(Map.entry(path, val));
				}
			}
			case final Collection<?> t -> {
				final var path = parentPath + "[]";
				for(final var e : t) todo.add(Map.entry(path, e));
			}
			default -> {
				if(v.getClass().isArray()) {
					final var l = Array.getLength(v);
					final var path = parentPath + "[]";
					for(var i=0;i<l;i++) todo.add(Map.entry(path, Array.get(v,i)));
				}
			}
			}
		}
		return objectInfo;
	}

	public static void main(final String[] argc) throws Throwable {
		final var bytes = Files.readAllBytes(Path.of("/1.json"));
		final var myEngine = JsonEngine.of(MetaConfig.DEFAULT);
		Object json;
		try(var s = myEngine.jsonInputStream(new ByteArrayInputStream(bytes))) { json = s.readObject(Object.class); }
		final var info = typeInfo(json);
		System.out.println("INFO "+info.size()+" entries");
		printJavaRecords(info);
	}
}