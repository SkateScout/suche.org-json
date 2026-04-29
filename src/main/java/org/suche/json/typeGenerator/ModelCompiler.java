package org.suche.json.typeGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

record ModelCompiler(
		Path       srcPath,
		String     basePackageName,
		Path       modelBase      ,
		List<Path> sourceFiles    ,
		Map<String, String> subPackages
		) implements AutoCloseable {

	private static String buildModuleInfo(final String basePackageName, final Map<String, String> subPackages) {
		final var sb = new StringBuilder("module org.suche.json.generated {\n");
		sb.append("\texports ").append(basePackageName).append(";\n");
		for (final var pkg : new TreeSet<>(subPackages.values())) {
			if (!pkg.isEmpty()) sb.append("\texports ").append(basePackageName).append(".").append(pkg).append(";\n");
		}
		return sb.append("}").toString();
	}

	static ModelCompiler of(final Path srcPath,final String basePackageName, final Map<String, String> subPackages) {
		final var srcGenBase      = srcPath.toAbsolutePath().normalize();
		final var modelBase       = srcGenBase.resolve(basePackageName.replace('.','/')).toAbsolutePath().normalize();
		return new ModelCompiler(srcPath, basePackageName, modelBase, new ArrayList<>(), subPackages);
	}

	public void add(final String relativeName, final String java) throws IOException {
		final var p = (basePackageName+"."+relativeName).replace('.', '/')+".java";
		final var destPath  = srcPath.resolve(p).toAbsolutePath().normalize();
		final var targetDir = destPath.getParent();
		if (!destPath.startsWith(modelBase)) throw new SecurityException("Path traversal attempt detected: " + destPath);
		Files.createDirectories(targetDir);
		Files.writeString(destPath, java);
		sourceFiles.add(destPath);
	}

	@Override public void close() throws Exception {
		add("module-info", buildModuleInfo(basePackageName, subPackages));
		final var compiler = javax.tools.ToolProvider.getSystemJavaCompiler();
		if (compiler != null && !sourceFiles.isEmpty()) {
			final var args = sourceFiles.stream().map(Path::toString).toArray(String[]::new);
			final var result = compiler.run(null, null, null, args);
			System.out.println("Auto-Compilation result: " + (result == 0 ? "SUCCESS" : "FAILED"));
		} else System.out.println("Skip Auto-Compilation: No system java compiler found.");
	}
}