# Vanilla Java JSON Engine 🚀

A hyper-optimized, zero-allocation, reflection-free JSON parser and serializer built for the modern JVM (JDK 26+). 

This engine was designed from the ground up to achieve maximum throughput and minimal garbage collection pressure, outperforming industry standards like Jackson in direct JMH benchmarks—without a single external dependency.

![Java Version](https://img.shields.io/badge/Java-26%2B-blue?logo=java)
![Dependencies](https://img.shields.io/badge/Dependencies-0-success)
![Loom Ready](https://img.shields.io/badge/Project_Loom-Ready-orange)
![License](https://img.shields.io/badge/License-MIT-green)

## ✨ Key Features

* **Blazing Fast (JMH Proven):** Consistently beats Jackson Databind in throughput and offers significantly lower jitter/variance under load.
* **Zero Dependencies:** 100% pure Vanilla Java. No `jackson`, no `gson`, no `org.json`. 
* **Zero-Allocation Hot-Path:** Utilizes advanced object pooling (`MetaPool`), custom cached byte arrays (`DIGITS` cache), and direct `byte[]` buffer pushing. The Garbage Collector rests while you parse millions of records.
* **Project Loom / Virtual Thread Ready:** Completely free of blocking locks or `synchronized` bottlenecks. Uses elastic soft-limit object pools instead of memory-heavy `ThreadLocal` structures to scale perfectly with millions of virtual threads.
* **Modern JDK Architecture:** Leverages Pattern Matching, Records, Sealed Interfaces, and most importantly: **`java.lang.invoke.MethodHandles` and `LambdaMetafactory`** instead of slow, traditional Reflection.
* **Smart Filtering & Transformers:** Enterprise-grade mapping capabilities (renaming, custom data formatting, ignoring fields) natively built into a fast, stateless Lambda-Pipeline.

---

## 📦 Installation

Add the library to your project. *(Note: Requires JDK 26 or higher)*

**Maven:**
```xml
<dependency>
    <groupId>org.suche.json</groupId>
    <artifactId>vanilla-json-engine</artifactId>
    <version>1.0.0</version>
</dependency>
```

**Gradle:**
```groovy
implementation 'org.suche.json:vanilla-json-engine:1.0.0'
```

---

## 📊 Performance (JMH Benchmark)

Tested against the industry standard `com.fasterxml.jackson.core:jackson-databind`.
*(Throughput in operations per second. Higher is better. Fork=1, Warmup=3, Iterations=5)*

| Engine | Score (ops/s) | Error Margin | Jitter / Stability |
| :--- | :--- | :--- | :--- |
| **Jackson (2.17.1)** | 3,457 | ± 0,365 | ~ 10.5% |
| **Suche JSON Engine** | **3,677** | **± 0,061** | **~ 1.6%** 🏆 |

**Why is it faster?**
1. **No Wrapper Objects:** Primitive values (`int`, `long`, `double`) are extracted using natively compiled `ToIntFunction` / `ToLongFunction` pointers via `LambdaMetafactory`, bypassing autoboxing entirely.
2. **Zero-Allocation Dates:** ISO-8601 timestamps are generated through bitwise operations and array lookups directly into the output stream. No `DateTimeFormatter` or `StringBuilder` allocations.
3. **Pre-computed Meta Registry:** Class metadata is resolved exactly once and cached in a highly concurrent, lock-free `JsonEngine` registry.
4. **Loop Unrolling:** Highly optimized processing of primitive arrays avoids branch-prediction failures deep in the CPU pipeline.

*(To run the benchmarks yourself, execute `./mvnw clean verify && java -jar target/benchmarks.jar`)*

---

## 🛠️ Quick Start

### 1. Initialize the Engine
Create a reusable, thread-safe instance of the engine. This acts as your high-speed cache.

```java
import org.suche.json.JsonEngine;
import org.suche.json.MetaConfig;

// Create the engine once (e.g., as a static constant or Spring Bean)
public static final JsonEngine ENGINE = JsonEngine.of(MetaConfig.DEFAULT);
```

### 2. Serialize (Write to Output)
```java
TargetClass myObject = new TargetClass("Data", 42);
ByteArrayOutputStream os = new ByteArrayOutputStream();

// Try-with-resources returns the stream and its context safely to the ObjectPool
try (var stream = ENGINE.jsonOutputStream(os)) {
    stream.writeObject(myObject);
}
```

### 3. Deserialize (Read from Input)
```java
InputStream in = /* your JSON source */;

try (var stream = ENGINE.jsonInputStream(in)) {
    TargetClass parsed = stream.readObject(TargetClass.class);
}
```

---

## ⚙️ Advanced Configuration (MetaConfig)

The engine provides extreme flexibility without bloating the core with annotations. Everything is controlled via the immutable `MetaConfig`.

### Custom Field Mapping & Value Formatting
You can dynamically rename keys, ignore fields, or even change their data types on the fly using the `mapField` / `mapComponent` lambdas.

```java
MetaConfig cfg = new MetaConfig(
    false, // emitClassName
    false, // skipDefaultValues
    null,  // mapComponent
    null,  // mapMethod
    (name, field) -> {
        // Example: Rename a field dynamically
        if (name.equals("internalId")) return "public_id";
        
        // Example: Ignore specific fields (Zero-Cost exception flow)
        if (field.isAnnotationPresent(MyIgnore.class)) throw MetaConfig.SKIP_FIELD;
        
        // Example: Transform a Date to Epoch MS on the fly
        if (field.getType() == Date.class) {
            return new MetaConfig.NameFilter(name, val -> ((Date)val).getTime());
        }
        return null; // Apply standard behavior
    },
    128, 5 * 1024 * 1024, 100_000, true
);

JsonEngine engine = JsonEngine.of(cfg);
```

### Legacy Integration (Transformers)
Need to process `org.json.JSONObject` or other legacy DOM nodes? Register a transformer outside the core engine. The engine transforms it instantly before parsing, keeping the core 100% dependency-free.

```java
// Tell the engine how to treat unknown 3rd-party objects
JsonOutputStream.registerTransformer(
    org.json.JSONObject.class, 
    obj -> obj.toMap() // Transforms to native java.util.Map
);
```

---

## 🏗️ Architecture Insights

* **The Static Cache Trap Avoided:** Caches are bound to the `JsonEngine` instance, not static classes. This prevents Metaspace memory leaks in application servers (Tomcat/WildFly) during hot-reloads.
* **FastKeyTable:** Property resolution relies on bit-masked custom hash tables (`FastKeyTable`) instead of standard HashMaps, eliminating `Map.Entry` allocations and hash collisions during parsing.
* **Fail-Fast Reflection:** Operations on final fields are handled cleanly without reflective mutation hacks, ensuring compatibility with the strict encapsulation rules of modern Java Modules (Jigsaw).
* **Elastic Soft-Limit Pools:** To guarantee stable throughput under Project Loom, internal buffers rely on non-blocking CAS queues with self-healing size limits instead of spin-locks or expensive thread-locals.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.