package org.suche.json.typeGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class SourceGenerator {
	private static final String INTERFACE_PREFIX = "public sealed interface ";
	private static final String ENUM_PREFIX      = "public enum ";
	private static final String RECORD_PREFIX    = "public record ";

	private static String getFQN(final String className, final Map<String, String> subPackages, final String basePackageName) {
		final var pkg = subPackages.getOrDefault(className, "");
		return pkg.isEmpty() ? basePackageName + "." + className : basePackageName + "." + pkg + "." + className;
	}

	public static GeneratedClass printInterfaces(final Map<String, ObjectInfo> classes
			, final Map<String, String> subPackages
			, final String interfaceName, final Set<String> permitsList) {

		final var referenced = new TreeSet<String>();
		final var simplePermits = new ArrayList<String>();

		for (final var className : permitsList) {
			referenced.add(className);
			simplePermits.add(className);
		}
		subPackages.put(interfaceName, "");
		// Fast-Path: First check if at least one property exists
		var noneEmpty = true;
		var oneProp = false;
		for (final var className : permitsList) {
			if(!(classes.get(className) instanceof final ObjectInfo oi)
					|| oi.properties == null
					|| oi.properties.size() == 0) {
				noneEmpty = false;
				break;
			}
			oneProp = true;
		}

		final var out = new StringBuilder(INTERFACE_PREFIX).append(interfaceName).append(" permits ").append(String.join(", ", simplePermits)).append(" { ");
		if(noneEmpty && oneProp) {
			Map<String, String> commonProps = null;
			for (final var className : permitsList) {
				final var oi = classes.get(className);
				final var count = oi.getMaxUseCount();
				final var currentProps = new TreeMap<String, String>();
				for (final var propEntry : oi.properties.entrySet()) {
					final var type = propEntry.getValue().resolveJavaType(count, false);
					currentProps.put(propEntry.getKey(), type);
					final var ref = propEntry.getValue().getReferencedClass();
					if (ref != null) referenced.add(ref);
				}
				if (commonProps == null) commonProps = currentProps;
				else {
					commonProps.keySet().retainAll(currentProps.keySet());
					final var it = commonProps.entrySet().iterator();
					while (it.hasNext()) {
						final var p = it.next();
						if (!p.getValue().equals(currentProps.get(p.getKey()))) it.remove();
					}
				}
				if (commonProps.isEmpty()) break;
			}
			if (commonProps != null && !commonProps.isEmpty()) {
				out.append("\n");
				for (final var prop : commonProps.entrySet())
					out.append("    ").append(prop.getValue()).append(" ").append(prop.getKey()).append("();\n");

			} else out.append(" ");
		}
		return new GeneratedClass(out.append("}").toString(), referenced);
	}

	private static String buildSource(final Map<String, String> subPackages, final String basePackageName, final String className, final Set<String> referencedClasses, final String body) {
		final var subPkg      = subPackages.getOrDefault(className, "");
		final var packageName = subPkg.isEmpty() ? basePackageName : basePackageName + "." + subPkg;

		final var imports = new TreeSet<String>();
		for (final String ref : referencedClasses)
			if (!ref.equals(className)
					&& (subPackages.get(ref) instanceof final String refPkg)
					&& !refPkg.equals(subPkg))
				imports.add(getFQN(ref, subPackages, basePackageName));

		final var java = new StringBuilder("package ").append(packageName).append(";\n\n");
		for (final String imp : imports) java.append("import ").append(imp).append(";\n");
		if (body.contains("Map<")) java.append("import java.util.Map;\n");
		if (body.contains("List<")) java.append("import java.util.List;\n");
		if (!imports.isEmpty() || body.contains("Map<") || body.contains("List<")) java.append("\n");
		java.append(body);
		return java.toString();
	}

	private static GeneratedClass printRecord(final Map<String, Set<String>> sealedInterfaces, final ObjectInfo oi) {
		final var count = oi.getMaxUseCount();
		final var fields = new ArrayList<String>();
		final var referenced = new TreeSet<String>();

		oi.properties.forEach((name, prop) -> {
			fields.add(prop.resolveJavaType(count, false) + " " + name);
			final var ref = prop.getReferencedClass();
			if (ref != null) referenced.add(ref);
		});

		final var out = new StringBuilder(RECORD_PREFIX).append(oi.className).append("(").append(String.join(", ", fields)).append(")");
		final var implement = new TreeSet<String>();
		sealedInterfaces.forEach((iName, permits) -> {
			if(permits.contains(oi.className)) {
				implement.add(iName);
				referenced.add(iName);
			}
		});
		if(!implement.isEmpty()) out.append(" implements ").append(String.join(", ", implement));
		return new GeneratedClass(out.append(" { }").toString(), referenced);
	}

	record GeneratedClass(String body, Set<String> references) {}

	static void writeFSourceFiles(final ModelCompiler c, final Map<String, ObjectInfo> classes
			, final Map<String, Set<String>>enums, final Map<String, Set<String>> sealedInterfaces
			, final String basePackageName, final Map<String, String> subPackages
			) throws IOException {

		final var javaSources = new TreeMap<String, GeneratedClass>();

		for(final var oi : classes.values()) {
			if(!oi.hasConflict && !"<MAP>".equals(oi.className))
				javaSources.put(oi.className, printRecord(sealedInterfaces, oi));
		}

		for(final var e : enums.entrySet()) {
			javaSources.put(e.getKey(), new GeneratedClass(ENUM_PREFIX + e.getKey() + " { " + String.join(", ", e.getValue()) + " }", Set.of()));
		}

		for (final var entry : sealedInterfaces.entrySet()) {
			final var interfaceName = entry.getKey();
			final var permitsList   = entry.getValue();
			javaSources.put(interfaceName, printInterfaces(classes, subPackages, interfaceName, permitsList));
		}

		for (final var entry : javaSources.entrySet()) {
			final var className = entry.getKey();
			if (!PrettyNaming.isValidVariableName(className)) throw new SecurityException("Invalid class name detected: " + className);
			final var genClass    = entry.getValue();
			final var subPkg      = subPackages.getOrDefault(className, "");
			storeSource(c, subPkg, className, buildSource(subPackages, basePackageName, className, genClass.references, genClass.body));
		}
	}

	private static void storeSource(final ModelCompiler c, final String subPkg, final String className, final String source) throws IOException {
		c.add(subPkg.isEmpty() ? className : subPkg + "." + className, source);
	}
}