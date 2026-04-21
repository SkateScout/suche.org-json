# Vanilla Java JSON Engine 🚀

A hyper-optimized, zero-allocation, reflection-free JSON parser and serializer built for the modern JVM (JDK 26+). 

This engine was designed from the ground up to achieve maximum throughput and minimal garbage collection pressure, outperforming industry standards like Jackson in direct JMH benchmarks—without a single external dependency.

![Java Version](https://img.shields.io/badge/Java-26%2B-blue?logo=java)
![Dependencies](https://img.shields.io/badge/Dependencies-0-success)
![Loom Ready](https://img.shields.io/badge/Project_Loom-Ready-orange)

## ✨ Key Features

* **Blazing Fast (JMH Proven):** Consistently beats Jackson Databind in throughput and offers significantly lower jitter/variance under load.
* **Zero Dependencies:** 100% pure Vanilla Java. No `jackson`, no `gson`, no `org.json`. 
* **Zero-Allocation Hot-Path:** Utilizes advanced object pooling, custom cached byte arrays, and direct `byte[]` buffer pushing. The Garbage Collector rests while you parse millions of records.
* **Project Loom / Virtual Thread Ready:** Completely free of blocking locks or `synchronized` bottlenecks in the hot-path. Uses elastic soft-limit object pools instead of memory-heavy `ThreadLocal` structures.
* **Modern JDK Architecture:** Leverages Pattern Matching, Records, Sealed Interfaces, and **`LambdaMetafactory`** instead of slow, traditional Reflection.
* **Hybrid Stack Architecture:** Exploits fast native JVM call stacks for typical JSON depth, while falling back to a safe, heap-based state machine for infinite-depth protection.

---

## 📦 Installation (via JitPack)

Add the library to your project. *(Note: Requires JDK 26 or higher)*

**Maven:**
Add the JitPack repository to your `pom.xml`:
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

Then add the dependency:
<dependency>
    <groupId>com.github.SkateScout</groupId>
    <artifactId>suche.org-json</artifactId>
    <version>v1.0.0</version>
</dependency>

**Gradle:**
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.SkateScout:suche.org-json:v1.0.0'
}

---

## 📊 Performance (JMH Benchmark)

Tested against `com.fasterxml.jackson.core:jackson-databind`. 
Benchmarks use the hybrid recursion approach (`maxRecursion=128`) on JDK 26.
*(Throughput in operations per second. Higher is better.)*

### Deserialization (Read)
| Dataset | Jackson (ops/s) | Suche Engine (ops/s) | Result |
| :--- | :--- | :--- | :--- |
| **Canada** (GeoJSON) | 38,624 | **56,342** | **+ 45% Faster** 🚀 |
| **Twitter** (Web API) | 473,346 | **520,496** | **+ 10% Faster** 🚀 |
| **Citm Catalog** | **234,858** | 231,188 | ~ Equal |

### Serialization (Write)
| Dataset | Jackson (ops/s) | Suche Engine (ops/s) | Result |
| :--- | :--- | :--- | :--- |
| **Canada** (GeoJSON) | 58,893 | **120,305** | **+ 104% Faster** 🚀 |
| **Citm Catalog** | 412,337 | **475,716** | **+ 15% Faster** 🚀 |
| **Twitter** (Web API) | **935,974** | 793,936 | - 15% |

---

## 🛠️ Quick Start

### 1. Initialize the Engine
import org.suche.json.JsonEngine;
import org.suche.json.MetaConfig;

public static final JsonEngine ENGINE = JsonEngine.of(MetaConfig.DEFAULT);

### 2. Serialize (Write)
try (var stream = ENGINE.jsonOutputStream(outputStream)) {
    stream.writeObject(myObject);
}

### 3. Deserialize (Read)
try (var stream = ENGINE.jsonInputStream(inputStream)) {
    TargetClass parsed = stream.readObject(TargetClass.class);
}

---

## 🏗️ Architecture Insights

* **Fractional Truncation & Bitmasking:** Bypasses `Double.parseDouble()` and branch-heavy `String` operations completely in the hot-path.
* **FastKeyTable:** Property resolution relies on bit-masked custom hash tables instead of standard HashMaps, eliminating `Map.Entry` allocations.
* **Fail-Fast Reflection:** Operations on final fields are handled cleanly without reflective mutation hacks, ensuring compatibility with Jigsaw modules.
* **The Static Cache Trap Avoided:** Caches are bound to the `JsonEngine` instance, preventing Metaspace memory leaks during hot-reloads in application servers.

---

## 📄 License
This project is licensed under the MIT License.
