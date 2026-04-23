package org.suche.json.benchmark;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;

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

import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(  value = 1,
jvmArgsAppend = {
		"-XX:+UnlockDiagnosticVMOptions",
		"-XX:+LogCompilation",
		"-XX:LogFile=jit_metafactory.log",
})
public class JsonSerializationBenchmark {
	// "--add-exports", "org.suche.json/org.suche.json=ALL-UNNAMED",
	// "--add-opens",   "org.suche.json/org.suche.json=ALL-UNNAMED"
	private ObjectMapper jacksonMapper;
	private JsonEngine myEngine;
	private Object testData;
	private ByteArrayOutputStream myOs;
	private ByteArrayOutputStream jacksonOs;

	@Param({"canada.json", "citm_catalog.json", "twitter.json"})
	public String fileName;

	@Setup(Level.Trial)
	public void setup() throws Exception {
		jacksonMapper = new ObjectMapper();
		jacksonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		myEngine = JsonEngine.of(MetaConfig.DEFAULT);
		myOs = new ByteArrayOutputStream(50 * 1024 * 1024);
		jacksonOs = new ByteArrayOutputStream(50 * 1024 * 1024);
		byte[] jsonData;
		try (var is = getClass().getClassLoader().getResourceAsStream(fileName)) {
			if (is == null) throw new RuntimeException("File not found: " + fileName);
			jsonData = is.readAllBytes();
		}
		testData = jacksonMapper.readValue(jsonData, Object.class);
	}

	// @Benchmark
	public void benchmarkJackson(final Blackhole bh) throws Exception {
		jacksonOs.reset();
		jacksonMapper.writeValue(jacksonOs, testData);
		bh.consume(jacksonOs.size());
	}

	// @Benchmark
	public void benchmarkFastjson2(final Blackhole bh) {
		final var bytes = JSON.toJSONBytes(testData);
		bh.consume(bytes);
	}

	// @Benchmark
	public void benchmarkMyEngine(final Blackhole bh) throws Exception {
		myOs.reset();
		try (var s = myEngine.jsonOutputStream(myOs)) { s.writeObject(testData); }
		bh.consume(myOs.size());
	}
}