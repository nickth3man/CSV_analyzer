

# COMPREHENSIVE REMEDIATION & ROADMAP PLAN

**Document Status:** Active
**Project:** NBA Expert (Text-to-SQL Agent)
**Based On:** Comprehensive Codebase Audit Report

---

## 1. Executive Summary

This plan outlines the strategy to address technical debt, security vulnerabilities, and architectural limitations identified in the codebase audit. The remediation is prioritized by risk and impact, focusing first on critical security flaws and performance bottlenecks, followed by architectural resilience, and finally feature enhancements.

**Key Objectives:**
1.  Eliminate **HIGH** severity security risks (SQL Injection).
2.  Resolve performance bottlenecks caused by synchronous I/O blocking.
3.  Improve system resilience against external API failures.
4.  Improve code maintainability by removing test artifacts from production.

---

## 2. Implementation Roadmap

### Phase 1: Critical Security & Stability (Week 1)
*Focus: Immediate patching of vulnerabilities and removal of blocking code.*

| Task | Description | File(s) | Est. Effort |
| :--- | :--- | :--- | :--- |
| **1.1 Fix SQL Injection** | Replace string interpolation with strict validation/parameterization. | `src/backend/utils/duckdb_client.py` | 4h |
| **1.2 Remove Mock Code** | Extract `_mock_response()` to test fixtures. | `src/backend/utils/call_llm.py` | 2h |
| **1.3 Async LLM Wrapper** | Implement `call_llm_async` using `httpx`. | `src/backend/utils/call_llm.py` | 8h |
| **1.4 LLM Circuit Breaker** | Add `@circuit_breaker` to prevent cascade failures. | `src/backend/utils/call_llm.py` | 4h |
| **1.5 Fallback Pattern** | Implement `exec_fallback()` in SQLGenerator. | `src/backend/nodes/sql_generator.py` | 3h |

### Phase 2: Architecture & Resilience (Week 2)
*Focus: Robustness, standardizing retry logic, and external client hygiene.*

| Task | Description | File(s) | Est. Effort |
| :--- | :--- | :--- | :--- |
| **2.1 NBA Client Refactor** | Add async, pooling, retry logic, and circuit breaker. | `src/backend/utils/nba_api_client.py` | 12h |
| **2.2 Response Caching** | Implement LRU/Redis caching with TTL to reduce costs. | `src/backend/utils/call_llm.py`, `cache.py` | 6h |
| **2.3 Standardized Backoff** | Replace `time.sleep` with `tenacity` library. | `src/backend/utils/call_llm.py` | 2h |
| **2.4 API Key Validation** | Add format checks and graceful failure for invalid keys. | `src/frontend/handlers.py` | 2h |
| **2.5 Input Sanitization** | Add basic validation for user questions to mitigate prompt injection. | Node files | 3h |

### Phase 3: Code Hygiene & Authentication (Week 3)
*Focus: Maintainability, security access control, and cleaning dead code.*

| Task | Description | File(s) | Est. Effort |
| :--- | :--- | :--- | :--- |
| **3.1 Linter Enablement** | Gradually remove ignored complexity rules (`PLR0912`, etc.). | `pyproject.toml` | 8h |
| **3.2 User Authentication** | Implement OAuth/JWT for the Chainlit interface. | `src/frontend/handlers.py` | 16h |
| **3.3 Remove Dead Code** | Clean unused imports and unused test mocks. | `src/tests/conftest.py`, Nodes | 4h |
| **3.4 Audit Logging** | Log all executed SQL with user context/timestamps. | `src/backend/utils/duckdb_client.py` | 4h |

### Phase 4: Performance & Features (Future)
*Focus: Enhancing UX and scaling capabilities.*

| Task | Description | File(s) | Est. Effort |
| :--- | :--- | :--- | :--- |
| **4.1 Response Streaming** | Token-by-token streaming for faster perceived latency. | `src/backend/utils/call_llm.py` | 12h |
| **4.2 Visualization Nodes** | Generate charts/graphs from query results. | `src/backend/nodes/chart_generator.py` | 16h |
| **4.3 Multi-User Memory** | Isolate conversation contexts per user session. | `src/backend/utils/memory.py` | 6h |
| **4.4 Batch API Support** | Optimize data population with batch fetching. | `src/backend/utils/nba_api_client.py` | 8h |

---

## 3. The Master Checklist

Use this checklist to track the completion of every item identified in the audit.

### 🔴 Priority 1: Critical & Safety
- [ ] **Extract Mock Responses:** Move `_mock_response()` from `src/backend/utils/call_llm.py` to `src/tests/fixtures/mock_llm_responses.py`.
- [ ] **Fix SQL Injection (Security):** Audit all `f"SELECT... FROM {table}"` queries in `src/backend/utils/duckdb_client.py` and implement whitelisting or parameterization.
- [ ] **Implement Async LLM:** Refactor `call_llm()` to support non-blocking execution using `httpx.AsyncClient`.
- [ ] **Add LLM Circuit Breaker:** Apply `@circuit_breaker(threshold=5, recovery=120)` to the LLM utility function.
- [ ] **Implement Response Caching:** Integrate caching logic with 24h TTL into the LLM call chain.
- [ ] **Implement Fallback:** Override `exec_fallback()` in `SQLGenerator` to handle max retries gracefully.

### 🟠 Priority 2: Architecture & Refactoring
- [ ] **Refactor NBA Client:**
    - [ ] Add async support.
    - [ ] Implement connection pooling.
    - [ ] Add retry logic with exponential backoff.
    - [ ] Add circuit breaker pattern.
- [ ] **Standardize Backoff:** Replace `time.sleep(2 ** (attempt + 1))` with the `tenacity` library.
- [ ] **Strict Linting:** Begin refactoring complex node functions to allow removal of `PLR0912`, `PLR0911`, and `C901` from ignore list in `pyproject.toml`.
- [ ] **Add Authentication:** Implement user login for the Chainlit frontend.
- [ ] **Sanitize Inputs:** Add validation layer for user questions before passing to LLM.

### 🟢 Priority 3: Features & Enhancement
- [ ] **Add Response Streaming:** Implement OpenAI-compatible streaming interface.
- [ ] **Create Visualization Nodes:** Design and implement nodes for `matplotlib` chart generation.
- [ ] **Isolate User Memory:** Refactor `memory.py` to handle multi-user contexts.
- [ ] **Implement Batch APIs:** Add batch data fetching methods to the NBA client.
- [ ] **Add Audit Logging:** Ensure every SQL execution is logged with timestamp and user context.

### 🧹 Code Hygiene & Dead Code Removal
- [ ] **Remove Unused Imports:** Run Ruff on `src/backend/nodes/` and `src/frontend/` to remove unused imports (e.g., `Any`, `TYPE_CHECKING`).
- [ ] **Clean Test Fixtures:** Remove or utilize `mock_matplotlib` and `mock_circuit_breaker` in `conftest.py`.
- [ ] **Utilize Utilities:** Integrate functions from `src/backend/utils/cache.py` and `resilience.py` or remove if unused.
- [ ] **Verify Placeholder Nodes:** Review `CombineResults` and ensure functionality matches documentation.

### 🔒 Security Hardening
- [ ] **Validate API Keys:** Add regex or format validation for OpenRouter keys on the frontend.
- [ ] **Path Traversal Check:** Audit `Path()` operations involving user input to ensure directory traversal is impossible.
- [ ] **Database Audit Trail:** Confirm read-only status is enforced and audit logs are capturing activity.

---

## 4. Risk Assessment & Mitigation

| Risk | Impact | Mitigation Strategy |
| :--- | :--- | :--- |
| **Breaking Changes during Async Refactor** | High | Run full integration test suite (`pytest`) after modifying `call_llm.py`. Maintain synchronous wrapper temporarily if needed. |
| **Linting Enabling Causes Build Fail** | Medium | Enable rules one at a time. Fix specific files before committing to strict `pyproject.toml` changes. |
| **External API Changes (OpenRouter/NBA)** | Medium | Ensure circuit breakers are tested. Mock external services reliably in unit tests. |
| **Performance Regression (Caching Overhead)** | Low | Monitor cache hit/miss ratios. Ensure caching logic does not add more latency than a standard API call for cold starts. |