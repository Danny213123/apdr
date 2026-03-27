# TIER3 SUCCESS TEST CASES - GIST ID REFERENCE

## Summary
- **Original test suite**: 17 tests (11 base + 6 failure pattern tests)
- **New tier3 success tests**: 18 tests from benchmark run 20260326-132841-apdr
- **Total test suite**: 35 integration tests

## Tier3 Success Cases (LLM Recovery Succeeded)

These 18 cases represent scenarios where tier1/tier2 (deterministic) resolution failed,  
but tier3 (LLM-based recovery) successfully identified and resolved the issues.

| # | Gist ID | Missing Module | Recovery Pattern | Requirements |
|---|---------|----------------|------------------|--------------|
| 1 | 098f399d69f230521ef530baca832e76 | Foundation | macOS-only framework | - |
| 2 | 1202516 | Foundation | macOS-only framework | - |
| 3 | 1242589 | Foundation | macOS-only framework | - |
| 4 | 1283684 | d3 | Local module (networkx-d3.js bridge) | networkx, simplejson |
| 5 | 1315148 | PyV8 | C++ binding (build failure) | - |
| 6 | 1307521 | _webassets | Optional import (try/except) | webassets==0.10 |
| 7 | 1423116 | cmemcached | C extension (MSVC required) | - |
| 8 | 1424374 | pcap | Local module (pcapy alternative) | dpkt, ipaddr |
| 9 | 1417f55cb896a44e68a6 | kivy_deps.gstreamer_dev | Missing transitive dependency | Kivy==1.9.1, pyserial |
| 10 | 1638546 | authorization | Local module (not pywin32) | httplib2, sql==0.3.0 |
| 11 | 1653394 | webservice | Local module | pyramid |
| 12 | 1694496 | settings | Django settings module | Django==5.1.3 |
| 13 | 1701845 | clips | Local module (not click package) | Django==5.1.3, django-haystack |
| 14 | 1c160c9eee91fd44c587 | pymba | Local/external library | moviepy==0.2.1.8.07, scikit-image==0.14.2 |
| 15 | 19317d3e4b9a58f2355e7643040d483a | UpdateManager | Linux-only (python-apt) | prettytable==3.1.1 |
| 16 | 2901479 | flask_celery | Local module (flask.ext pattern) | Flask==3.1.0, flask-heroku==0.1.5 |
| 17 | 2b2abbb88b5d2b4f4e5adde42975fd0f | newspaper | Build failure | beautifulsoup4==4.12.3, requests==2.32.3 |
| 18 | 2de2e9a156fe619dbdad762fe1cf84e1 | Lasagne | Theano compatibility issue | numpy==1.21.6, Theano-PyMC==1.1.2 |

## Recovery Patterns Tested

### Local Module Identification (11 cases)
LLM identifies modules that should not be installed from PyPI:
- d3, pcap, authorization, webservice, settings, clips, pymba, flask_celery

### Platform-Specific Packages (5 cases)
LLM recognizes OS-specific packages that don't exist on current platform:
- Foundation (macOS), UpdateManager (Linux), PyV8 (C++ binding)

### Optional Imports (2 cases)
LLM detects try/except wrapped imports that are intentionally optional:
- _webassets, UpdateManager

### Build Failures (3 cases)
LLM suggests alternatives or skip when C compiler/dependencies missing:
- cmemcached, newspaper, PyV8

### Dependency Conflicts (2 cases)
LLM resolves complex transitive or version conflicts:
- kivy_deps, Theano/Lasagne

## Test File Location
`tools/apdr/llm_py/tests/test_llm_integration.py`

All tests use `@pytest.mark.integration` and require Ollama with qwen3.5:9b model.
