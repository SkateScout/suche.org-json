package org.suche.json.benchmark;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.suche.json.JsonEngine;
import org.suche.json.MetaConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(
		value = 1,
		jvmArgsAppend = {
				"-XX:+UnlockDiagnosticVMOptions",
				"-XX:+LogCompilation",
				"-XX:LogFile=jit_metafactory.log",
				"--add-exports", "org.suche.json/org.suche.json=ALL-UNNAMED",
				"--add-opens",   "org.suche.json/org.suche.json=ALL-UNNAMED"
		}
		)
public class JsonDeserializationBenchmark {

	public record CitmCatalog(
			Map<String, Object> events,
			Map<String, String> venueNames,
			List<Map<String, String>> performances
			) {}

	private ObjectMapper jacksonMapper;
	private JsonEngine myEngine;
	private Class<?> dest;
	private byte[] jsonData;

	@Param({"canada.json", "citm_catalog.json", "twitter.json"}) public String fileName;
	@Param({"0","128"}) public int  maxRecursion;

	@Setup(Level.Trial)
	public void setup() throws Exception {
		dest = Object.class;
		jacksonMapper = new ObjectMapper();
		myEngine = JsonEngine.of(MetaConfig.DEFAULT);
		myEngine.maxRecursiveDepth(maxRecursion);
		try (var is = getClass().getClassLoader().getResourceAsStream(fileName)) {
			if (is == null) throw new RuntimeException("File not found: " + fileName);
			jsonData = is.readAllBytes();
		}
	}

	// --- Benchmark 1: Jackson ---
	@Benchmark
	public void benchmarkJackson(final Blackhole bh) throws Exception {
		final var parsedTree = jacksonMapper.readValue(jsonData, dest);
		bh.consume(parsedTree);
	}

	// --- Benchmark 2: suche.org Engine ---
	@Benchmark
	public void benchmarkMyEngine(final Blackhole bh) throws Exception {
		try (var is = myEngine.jsonInputStream(new ByteArrayInputStream(jsonData))) {
			final var parsedTree = is.readObject(dest);
			bh.consume(parsedTree);
		}
	}
}