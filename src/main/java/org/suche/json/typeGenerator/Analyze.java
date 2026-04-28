package org.suche.json.typeGenerator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.suche.json.JsonEngine;
import org.suche.json.MetaConfig;

public final class Analyze {
	static final int    MIN_PREFIX_LEN  = 2;
	static final int    MIN_POSTIFX_LEN = 3;
	private static final int    PROPERTY_COUNT_THRESHOLD = 32;
	private static final String ENUM_PREFIX      = "public enum ";
	private static final String INTERFACE_PREFIX = "public sealed interface ";

	private static void optimizeDynamicMaps(final Map<String, ObjectInfo> classes) {
		final var iterator = classes.entrySet().iterator();
		while (iterator.hasNext()) {
			final var entry = iterator.next();
			final var className = entry.getKey();
			final var oi = entry.getValue();

			if (oi.properties == null || oi.properties.size() < PROPERTY_COUNT_THRESHOLD) continue;

			var firstType = -1;
			var allSame = true;
			PropertyInfo sampleProp = null;

			for (final var prop : oi.properties.values()) {
				if (firstType == -1) {
					firstType = prop.types;
					sampleProp = prop;
				} else if (firstType != prop.types) {
					allSame = false;
					break;
				}
			}

			if (allSame && sampleProp != null) {
				rewriteObjectToMapReferences(classes, className, sampleProp);
				iterator.remove();
			}
		}
	}

	private static void rewriteObjectToMapReferences(final Map<String, ObjectInfo> classes, final String targetClass, final PropertyInfo mapValueProp) {
		for (final var oi : classes.values()) {
			if (oi.properties == null) continue;
			for (final var prop : oi.properties.values()) {
				if ((prop.types & PropertyInfo.T_OBJECT) != 0 && prop.classes != null && prop.classes.contains(targetClass)) {
					prop.classes.remove(targetClass);
					prop.types &= ~PropertyInfo.T_OBJECT;
					prop.types |= PropertyInfo.T_MAP;
					if (prop.mapValues == null) {
						prop.mapValues = new PropertyInfo(prop.fieldName);
						prop.mapValues.types = mapValueProp.types;
						prop.mapValues.classes = mapValueProp.classes;
						prop.mapValues.arrayElements = mapValueProp.arrayElements;
					}
				}
			}
		}
	}

	static void collectSealedInterfaces(final PropertyInfo prop, final Map<String, Set<String>> sealedInterfaces, final Set<String> globalNames) {
		if (prop == null) return;
		final var allTypes = new TreeSet<String>();
		if (prop.classes != null) allTypes.addAll(prop.classes);
		if (prop.enumClasses != null) allTypes.addAll(prop.enumClasses);
		if (allTypes.size() > 1) {
			prop.interfaceName = PrettyNaming.findSealedInterfaceName(allTypes, prop.fieldName.replace("[]", ""), globalNames);
			sealedInterfaces.computeIfAbsent(prop.interfaceName, _->new TreeSet<>()).addAll(allTypes);
		}
		collectSealedInterfaces(prop.arrayElements, sealedInterfaces, globalNames);
		collectSealedInterfaces(prop.mapValues, sealedInterfaces, globalNames);
	}

	public static void printInterfaces(final Map<String, String> javaSources, final Map<String, ObjectInfo> classes, final Map<String, Set<String>> sealedInterfaces, final Map<String, String> subPackages, final String basePackageName) {
		for (final var entry : sealedInterfaces.entrySet()) {
			final var interfaceName = entry.getKey();
			final var permitsList = entry.getValue();
			final var fqnPermits = new ArrayList<String>();
			for (final var className : permitsList) {
				final var pkg = subPackages.getOrDefault(className, "");
				fqnPermits.add(pkg.isEmpty() ? basePackageName + "." + className : basePackageName + "." + pkg + "." + className);
			}
			subPackages.put(interfaceName, "");

			Map<String, String> commonProps = null;
			for (final var className : permitsList) {
				final var oi = classes.get(className);
				if (oi == null || oi.properties == null) {
					if (commonProps != null) commonProps.clear();
					else commonProps = new TreeMap<>();
					break;
				}
				final var count = oi.getMaxUseCount();
				final var currentProps = new TreeMap<String, String>();
				for (final var propEntry : oi.properties.entrySet()) {
					currentProps.put(propEntry.getKey(), propEntry.getValue().resolveJavaType(count, false));
				}
				if (commonProps == null) {
					commonProps = currentProps;
				} else {
					commonProps.keySet().retainAll(currentProps.keySet());
					final var it = commonProps.entrySet().iterator();
					while (it.hasNext()) {
						final var p = it.next();
						if (!p.getValue().equals(currentProps.get(p.getKey()))) it.remove();
					}
				}
				if (commonProps.isEmpty()) break;
			}
			final var out = new StringBuilder(INTERFACE_PREFIX).append(interfaceName).append(" permits ").append(String.join(", ", fqnPermits)).append(" {");
			if (commonProps != null && !commonProps.isEmpty()) {
				out.append("\n");
				for (final var prop : commonProps.entrySet()) {
					out.append("    ").append(prop.getValue()).append(" ").append(prop.getKey()).append("();\n");
				}
			} else out.append(" ");
			javaSources.put(interfaceName, out.append("}").toString());
		}
	}

	static void optimizeAutoEnums(final Map<String, ObjectInfo> classes, final Map<String, Set<String>> globalEnums, final Map<String, Set<String>> sealedInterfaces) {
		final var globalNames = new HashSet<>(classes.keySet());
		globalNames.addAll(globalEnums     .keySet());
		globalNames.addAll(sealedInterfaces.keySet());
		final var enumGroups = new HashMap<Set<String>, java.util.List<PropertyInfo>>();
		for (final var oi : classes.values()) {
			if (oi.properties == null) continue;
			for (final var prop : oi.properties.values()) {
				if (prop.useCount >= 2048 && !prop.autoStringAborted && prop.autoStringValues != null && !prop.autoStringValues.isEmpty()) {
					enumGroups.computeIfAbsent(prop.autoStringValues, _ -> new ArrayList<>()).add(prop);
				}
			}
		}

		for (final var entry : enumGroups.entrySet()) {
			final var values = entry.getKey();
			final var props  = entry.getValue();
			final var fieldNames = new TreeSet<String>();
			for (final var prop : props) fieldNames.add(prop.fieldName);
			final var enumName = PrettyNaming.findCommonName(fieldNames, globalNames);
			final var finalName = PrettyNaming.findUniqueName(enumName, globalNames);
			globalNames.add(finalName);
			globalEnums.put(finalName, new TreeSet<>(values));
			for (final var prop : props) {
				prop.types         = PropertyInfo.T_ENUM;
				prop.interfaceName = finalName;
			}
		}
	}

	private static Object readJson(final Path jsonPath) throws Throwable {
		final var myEngine = JsonEngine.of(MetaConfig.DEFAULT);
		final var bytes = Files.readAllBytes(jsonPath);
		return myEngine.jsonInputStream(new ByteArrayInputStream(bytes)).readObject(Object.class);
	}

	private static void compile(final List<Path> compiledFiles) {
		final var compiler = javax.tools.ToolProvider.getSystemJavaCompiler();
		if (compiler != null && !compiledFiles.isEmpty()) {
			final var args = compiledFiles.stream().map(Path::toString).toArray(String[]::new);
			final var result = compiler.run(null, null, null, args);
			System.out.println("Auto-Compilation result: " + (result == 0 ? "SUCCESS" : "FAILED"));
		} else {
			System.out.println("Skip Auto-Compilation: No system java compiler found.");
		}
	}

	private static List<Path> writeFSourceFiles(final Map<String, ObjectInfo> classes
			, final Map<String, Set<String>>enums, final Map<String, Set<String>> sealedInterfaces
			, final Map<String, String> subPackages, final String basePackageName
			, final Path modelBase, final Path srcGenBase) throws IOException {

		final var javaSources = new TreeMap<String, String>();
		classes.values().forEach(oi -> { if(!oi.hasConflict && !"<MAP>".equals(oi.className)) oi.printRecord(javaSources, sealedInterfaces); });
		enums.forEach((name, vals) -> javaSources.put(name, ENUM_PREFIX + name + " { " + String.join(", ", vals) + " }"));
		printInterfaces(javaSources, classes, sealedInterfaces, subPackages, basePackageName);

		final var compiledFiles = new ArrayList<Path>();
		final var wildcardImports = new StringBuilder("import ").append(basePackageName).append(".*;\n");

		for (final var pkg : new TreeSet<>(subPackages.values())) {
			if (!pkg.isEmpty()) wildcardImports.append("import ").append(basePackageName).append(".").append(pkg).append(".*;\n");
		}

		for (final var entry : javaSources.entrySet()) {
			final var className = entry.getKey();
			final var source = entry.getValue();
			if (!PrettyNaming.isValidVariableName(className)) throw new SecurityException("Invalid class name detected: " + className);

			final var subPkg    = subPackages.getOrDefault(className, "");
			final var targetDir = subPkg.isEmpty() ? modelBase : modelBase.resolve(subPkg);
			final var destPath  = targetDir.resolve(className + ".java").toAbsolutePath().normalize();

			if (!destPath.startsWith(modelBase)) throw new SecurityException("Path traversal attempt detected: " + destPath);
			Files.createDirectories(targetDir);

			final var packageName = subPkg.isEmpty() ? basePackageName : basePackageName + "." + subPkg;
			final var imports = new StringBuilder(wildcardImports);
			if (source.contains("Map<")) imports.append("import java.util.Map;\n");

			final var fileContent = """
					package %s;

					%s
					%s
					""".formatted(packageName, imports.toString(), source);

			Files.writeString(destPath, fileContent);
			compiledFiles.add(destPath);
		}

		final var moduleExports = new StringBuilder();
		moduleExports.append("\texports ").append(basePackageName).append(";\n");
		for (final var pkg : new TreeSet<>(subPackages.values())) {
			if (!pkg.isEmpty()) {
				moduleExports.append("\texports ").append(basePackageName).append(".").append(pkg).append(";\n");
			}
		}

		final var moduleInfo = """
				module org.suche.json.generated {
				%s
				}
				""".formatted(moduleExports.toString());

		final var moduleInfoPath = srcGenBase.resolve("module-info.java").toAbsolutePath().normalize();
		Files.writeString(moduleInfoPath, moduleInfo);
		compiledFiles.add(moduleInfoPath);

		return compiledFiles;
	}

	public static void main(final String[] argc) throws Throwable {
		final var json = readJson(Path.of("/1.json"));

		final var enums   = new HashMap<String, Set<String>>();
		final var classes = new HashMap<String, ObjectInfo>();
		ObjectInfo.typeInfo(classes, enums, json);
		final var sealedInterfaces = new HashMap<String, Set<String>>();
		optimizeDynamicMaps(classes);
		optimizeAutoEnums(classes, enums, sealedInterfaces);
		final var globalNames = new HashSet<>(classes.keySet());
		globalNames.addAll(enums.keySet());

		for (final var oi : classes.values()) {
			if (oi.properties != null) for (final var p : oi.properties.values()) collectSealedInterfaces(p, sealedInterfaces, globalNames);
		}

		if (json instanceof Collection || (json != null && json.getClass().isArray())) {
			final var rootProp = new PropertyInfo("root");
			rootProp.register("root", enums, json);
			collectSealedInterfaces(rootProp, sealedInterfaces, globalNames);
		}

		final var allGeneratedTypes = new HashSet<String>();
		for (final var oi : classes.values()) {
			if (!oi.hasConflict && !"<MAP>".equals(oi.className)) allGeneratedTypes.add(oi.className);
		}
		allGeneratedTypes.addAll(enums.keySet());
		allGeneratedTypes.addAll(sealedInterfaces.keySet());
		final var subPackages = PrettyNaming.determineSubPackages(allGeneratedTypes);
		final var srcGenBase = Path.of("src/generated/java").toAbsolutePath().normalize();
		final var modelBase = srcGenBase.resolve("org/suche/json/mode/testDataset1").toAbsolutePath().normalize();
		final var basePackageName = srcGenBase.relativize(modelBase).toString().replace('/', '.').replace('\\', '.');
		final var compiledFiles = writeFSourceFiles(classes, enums, sealedInterfaces, subPackages, basePackageName, modelBase, srcGenBase);
		compile(compiledFiles);
	}

}