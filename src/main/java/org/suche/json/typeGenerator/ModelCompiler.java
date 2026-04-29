package org.suche.json.typeGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;

import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

record ModelCompiler(
		Path                targetJar      ,
		String              basePackageName,
		Map<String, String> subPackages    ,
		List<MemorySource>  sources
		) implements AutoCloseable {

	private static String buildModuleInfo(final String basePackageName, final Map<String, String> subPackages) {
		final var exports = new TreeSet<>(subPackages.values()).stream()
				.filter(pkg -> !pkg.isEmpty())
				.map(pkg -> "\texports " + basePackageName + "." + pkg + ";\n")
				.collect(Collectors.joining());

		// Using Java Text Blocks for cleaner multi-line string assembly
		return """
				module org.suche.json.generated {
				\texports %s;
				%s}
				""".formatted(basePackageName, exports);
	}

	static ModelCompiler of(final Path targetJarPath, final String basePackageName, final Map<String, String> subPackages) {
		// We no longer need modelBase or srcPath, only the target JAR path
		final var targetJar = targetJarPath.toAbsolutePath().normalize();
		return new ModelCompiler(targetJar, basePackageName, subPackages, new ArrayList<>());
	}

	public void add(final String relativeName, final String java) {
		// Store the java code in memory instead of writing to disk
		final var className = "module-info".equals(relativeName) ? relativeName : basePackageName + "." + relativeName;
		sources.add(new MemorySource(className, java));
	}

	@Override
	public void close() throws Exception {
		add("module-info", buildModuleInfo(basePackageName, subPackages));

		final var compiler = ToolProvider.getSystemJavaCompiler();
		if (compiler == null || sources.isEmpty()) {
			System.out.println("Skip Auto-Compilation: No system java compiler found or no sources.");
			return;
		}

		final var diagnostics = new DiagnosticCollector<JavaFileObject>();
		try (	final var standardFileManager = compiler.getStandardFileManager(diagnostics, null, null);
				final var memoryFileManager   = new MemoryFileManager(standardFileManager)) {

			// Run compilation task using our in-memory file manager
			final var task    = compiler.getTask(null, memoryFileManager, diagnostics, null, null, sources);
			final var success = task.call();

			if (null != success && success.booleanValue()) {
				writeJar(memoryFileManager.getCompiledClasses());
				System.out.println("Auto-Compilation result: SUCCESS. Saved to " + targetJar);
			} else {
				System.out.println("Auto-Compilation result: FAILED");
				diagnostics.getDiagnostics().forEach(System.out::println);
			}
		}
	}

	private void writeJar(final Map<String, byte[]> compiledClasses) throws IOException {
		Files.createDirectories(targetJar.getParent());
		try (	final var fos = Files.newOutputStream(targetJar);
				final var jos = new JarOutputStream(fos)) {
			for (final var entry : compiledClasses.entrySet()) {
				final var entryName = entry.getKey().replace('.', '/') + ".class";
				// Security check: Prevent path traversal / ZIP-Slip in JAR entries
				if (entryName.contains("..")) throw new SecurityException("Invalid class name detected, path traversal blocked: " + entryName);
				jos.putNextEntry(new JarEntry(entryName));
				jos.write(entry.getValue());
				jos.closeEntry();
			}
		}
	}

	/** Represents a Java source file held in memory. */
	private static final class MemorySource extends SimpleJavaFileObject {
		private final String code;

		MemorySource(final String name, final String code) {
			super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
			this.code = code;
		}

		@Override public CharSequence getCharContent(final boolean ignoreEncodingErrors) {
			return code;
		}
	}

	/** Represents a compiled Java class file held in memory. */
	private static final class MemoryByteCode extends SimpleJavaFileObject {
		private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

		MemoryByteCode(final String name) {
			super(URI.create("byte:///" + name.replace('.', '/') + Kind.CLASS.extension), Kind.CLASS);
		}

		@Override public OutputStream openOutputStream() { return baos; }
		byte[] getBytes() { return baos.toByteArray(); }
	}

	/** Custom file manager that intercepts class output and stores it in memory. */
	private static final class MemoryFileManager extends ForwardingJavaFileManager<JavaFileManager> {
		private final Map<String, MemoryByteCode> compiledClasses = new HashMap<>();

		MemoryFileManager(final JavaFileManager fileManager) { super(fileManager); }

		@Override public JavaFileObject getJavaFileForOutput(final Location location, final String className, final JavaFileObject.Kind kind, final FileObject sibling) {
			final var byteCode = new MemoryByteCode(className);
			compiledClasses.put(className, byteCode);
			return byteCode;
		}

		Map<String, byte[]> getCompiledClasses() {
			final var result = new HashMap<String, byte[]>();
			compiledClasses.forEach((name, byteCode) -> result.put(name, byteCode.getBytes()));
			return result;
		}
	}
}