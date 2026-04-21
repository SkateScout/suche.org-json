# Security Policy

First of all, thank you for taking the time to report a vulnerability. Security is taken very seriously in this project. As a high-performance JSON engine, we are especially focused on preventing resource exhaustion, denial-of-service (DoS) attacks, and buffer overflows.

## Supported Versions

Currently, the following versions of the Vanilla Java JSON Engine are actively supported with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.0   | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

If you believe you have found a security vulnerability in this project, please report it privately. You can do this by:

1. **GitHub Security Advisories:** Go to the "Security" tab of this repository, click on "Advisories", and click "Report a vulnerability" to open a private draft security advisory.
2. **Direct Contact:** Alternatively, you can send an email to: `SkateScout@gmail.com`

Please include the following information in your report:
* The type of vulnerability (e.g., DoS, memory leak, improper input validation).
* The exact version(s) of the library affected.
* Step-by-step instructions or a Proof of Concept (PoC) payload to reproduce the issue.
* Any specific JVM or OS constraints (e.g., "Only happens on JDK 26 on Windows").

### Response Timeline
* We aim to acknowledge your report within **48 hours**.
* We will verify the vulnerability and, if confirmed, work on a patch immediately.
* A CVE will be requested and published alongside the patched release.
* We will publicly acknowledge your contribution in the release notes (unless you prefer to remain anonymous).

## Scope

We are particularly interested in vulnerabilities related to:
* **Resource Exhaustion:** Payloads that cause exponential memory allocation or infinite CPU loops (e.g., bypassing the `maxDepth` or `maxStringLength` limits).
* **Information Disclosure:** Memory leaks that could expose data from previously parsed JSON documents due to object pool contamination.
* **Logic Flaws:** Severe misinterpretations of valid JSON that could lead to security-relevant data corruption downstream.
