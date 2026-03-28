# Phase 10: Unrecovered Gap Report

**Generated:** 2026-03-28
**Source:** `10-case-delta.json`
**Scope:** 70 canonical cases from the Phase 7 tier3 parity slice, all unrecovered after the targeted rerun.

## Dominant Failure Buckets

All 70 canonical cases remained in their baseline failure state. The breakdown by dominant failure bucket:

| Bucket | Cases | Share |
|--------|------:|------:|
| environment-build-failed | 21 | 30.0% |
| module-not-found | 19 | 27.1% |
| dependency-conflict | 12 | 17.1% |
| version-not-found | 11 | 15.7% |
| syntax-error | 5 | 7.1% |
| import-error | 2 | 2.9% |

## Canonical Cases By Bucket

### environment-build-failed (21 cases)

| Case ID | Validation Reason |
|---------|-------------------|
| `035dc3b722b7f89cce66520dde285c9a` | Repeated failure signature `BuildFailure\|TPL-OS\|\|pyeclib` across multiple dependency sets; ending recovery loop. |
| `10005117` | No automatic recovery fix found for BuildFailure. Error: `python setup.py egg_info` failed. |
| `1040366` | Stopped validation because requirements began oscillating. |
| `1042778` | Repeated failure signature `BuildFailure\|TPL-OS\|\|python-cjson` across multiple dependency sets; ending recovery loop. |
| `1077318` | No automatic recovery fix found for BuildFailure. Error: `python setup.py egg_info` failed (Python 2.7 env). |
| `1254809` | No automatic recovery fix found for BuildFailure. Error: `python setup.py egg_info` failed (Python 2.7 env). |
| `2038329` | No automatic recovery fix found for BuildFailure. Error: `python setup.py egg_info` failed (Python 2.7 env). |
| `21b4442b7e7f36f6a17b` | No automatic recovery fix found for Unknown. Error: subprocess-exited-with-error. |
| `2310005` | No automatic recovery fix found for BuildFailure. Error: `python setup.py egg_info` failed. |
| `2371c78fc0a5c8935a7a` | No automatic recovery fix found for BuildFailure. Error: `python setup.py egg_info` failed (Python 2.7 env). |
| `2628382` | Repeated failure signature `BuildFailure\|TPL-OS\|\|persistent` across multiple dependency sets; ending recovery loop. |
| `28bf77e9a95ae6b70b14141feacb1f84` | No automatic recovery fix found for BuildFailure. Error: subprocess-exited-with-error. |
| `309bed093f6a7084c855` | No automatic recovery fix found for Unknown. Error: subprocess-exited-with-error. |
| `33150bde6bd296310e41ea5d018fce51` | No automatic recovery fix found for SystemDependency. Error: twisted. |
| `33e2172bafbb5dd794ab` | No automatic recovery fix found for BuildFailure. Error: `python setup.py egg_info` failed (Python 2.7 env). |
| `3411495` | No automatic recovery fix found for Unknown. Error: AssertionError. |
| `3b1159baecb809b5fcb3a6154bc3cb0b` | No automatic recovery fix found for BuildFailure. Error: `python setup.py egg_info` failed (Python 2.7 env). |
| `3b71a120ae7789956ef8` | No automatic recovery fix found for BuildFailure. Error: `python setup.py egg_info` failed (Python 2.7 env). |
| `4074260` | Repeated failure signature `BuildFailure\|TPL-OS\|\|persistent` across multiple dependency sets; ending recovery loop. |
| `4108a54877406dc231d95514e538bde9` | Repeated failure signature `BuildFailure\|TPL-OS\|\|bluepy` across multiple dependency sets; ending recovery loop. |
| `4543974` | No automatic recovery fix found for SystemDependency. Error: twisted. |

### module-not-found (19 cases)

| Case ID | Validation Reason |
|---------|-------------------|
| `1068868` | Missing module `taggit_autocomplete` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `1231964e784ab9acb65d` | Runtime import failed: missing module `imp`. |
| `1433392` | Missing module `gisutils` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `1545255` | Missing module `djangorestframework` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `1561144` | Missing module `Stencil` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `1701845` | Missing module `clips` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `1823320` | Missing module `turbogears` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `1e2600ed62d5e76b21ee` | Runtime import failed: missing module `pkg_resources`. |
| `263113` | Missing module `pkg_resources` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `3001099` | Missing module `_distance_wrap` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `3310561` | Missing module `i3` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `3682135` | Missing module `Image` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `3799831` | Missing module `numpy` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `3805436` | Runtime import failed: missing module `elementtree`. |
| `3a6e4d618afc344aab81` | Runtime import failed: missing module `pkg_resources`. |
| `4093998b625d76ef4afe` | Missing module `ib` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `4882342eba2b57376ed1` | Runtime import failed: missing module `numpy.distutils`. |
| `4995164` | Missing module `pizzanuvola_teaser` persisted across multiple dependency sets; ending recovery as a mapping failure. |
| `4aed548e606f11971f5a` | Runtime import failed: missing module `api`. |

### dependency-conflict (12 cases)

| Case ID | Validation Reason |
|---------|-------------------|
| `0830affa1f7f19fd47b06d4cf89ed44d` | Dependency solver reported an incompatible version bundle: keras==3.0.0 vs tensorflow-intel 2.18.0 requires keras>=3.5.0. |
| `0a3d4fae965bdbec1f9d` | Dependency solver reported an incompatible version bundle: keras==3.0.0 vs tensorflow-intel 2.18.0 requires keras>=3.5.0. |
| `0bdd7059a08cbcd00898` | Dependency solver reported an incompatible version bundle: keras==3.0.0 vs tensorflow-intel 2.18.0 requires keras>=3.5.0. |
| `187895beb89f0a1b3a54` | Dependency solver reported an incompatible version bundle: keras==3.0.0 vs tensorflow-intel 2.18.0 requires keras>=3.5.0. |
| `19d2397ff8da1952556cf2417d965f6c` | Pinned package versions conflict with each other for the attempted validation environment. |
| `1d878d0401b28b281eb75016ed29f2ee` | Dependency solver reported an incompatible version bundle: keras==3.0.0 vs tensorflow-intel 2.18.0 requires keras>=3.5.0. |
| `3018bf3643f80798bde75c17571a38a9` | Pinned package versions conflict with each other for the attempted validation environment. |
| `31eee50b9aaebf387b380f70054575c5` | Dependency solver reported an incompatible version bundle: tensorboard==2.4.0 vs tensorflow-intel 2.18.0 requires tensorboard<2.19,>=2.18. |
| `342989` | Dependency solver reported an incompatible version bundle: numpy==1.26.4 vs pytensor 2.38.2 requires numpy>=2.0. |
| `3a2a081e4f3089920fd8aecefecbe280` | Dependency solver reported an incompatible version bundle: keras==3.0.0 vs tensorflow-intel 2.18.0 requires keras>=3.5.0. |
| `3d99498d4236248f9bfbc8ed2fd424fa` | Pinned package versions conflict with each other for the attempted validation environment. |
| `3fdd80a08808bd275142d46863e92d68` | Dependency solver reported an incompatible version bundle: keras==3.0.0 vs tensorflow-intel 2.18.0 requires keras>=3.5.0. |

### version-not-found (11 cases)

| Case ID | Validation Reason |
|---------|-------------------|
| `1191457` | Package `odfpy==0.9` is unavailable for the selected Python version. |
| `1248728` | No matching distribution found for PyJWT>=2.0.0 (from social-auth-core). |
| `1440754` | No matching distribution found for python-dateutil<2.0,>=2.1 (from github2). |
| `1b49c03968b2c83897a4a15c78980b18` | Repeated contradictory pins for `torch` exhausted recovery; ending compatibility retries. |
| `2977d9f26866b05583b0c40d88a315bf` | Repeated contradictory pins for `torchvision` exhausted recovery; ending compatibility retries. |
| `2b19fd6f758ffd2e8ab9ec7d1f3f4b2c` | Repeated contradictory pins for `torch` exhausted recovery; ending compatibility retries. |
| `2de2e9a156fe619dbdad762fe1cf84e1` | Package `numpy==1.21.6` is unavailable for the selected Python version. |
| `2e3b989e0343f0884388ed7ed82eb3b0` | Repeated contradictory pins for `torch` exhausted recovery; ending compatibility retries. |
| `3829194` | No matching distribution found for setuptools>=58.0.0. |
| `4089133` | Repeated contradictory pins for `mitmproxy` exhausted recovery; ending compatibility retries. |
| `4133c66ccf65c0ba1f5f5a5bc4fb7298` | Repeated contradictory pins for `torch` exhausted recovery; ending compatibility retries. |

### syntax-error (5 cases)

| Case ID | Validation Reason |
|---------|-------------------|
| `1042719` | Stopped validation because requirements began oscillating. |
| `2888158` | Stopped validation because requirements began oscillating. |
| `2bcca0a7654168ef454f` | Stopped validation because requirements began oscillating. |
| `2d4a4a8be57a9b8e94c7a4903d8d8bf8` | Stopped validation because requirements began oscillating. |
| `3153844` | Stopped validation because requirements began oscillating. |

### import-error (2 cases)

| Case ID | Validation Reason |
|---------|-------------------|
| `10938795` | Runtime import failed: ImportError cannot import name `parse_rule` from `werkzeug.routing`. |
| `4145581` | Import error during runtime validation (no detailed reason recorded). |

## Follow-On Notes

### environment-build-failed (21 cases)

The largest bucket. Three dominant sub-patterns:

1. **Python 2.7 setup.py failures (7 cases):** Cases `1077318`, `1254809`, `2038329`, `2371c78fc0a5c8935a7a`, `33e2172bafbb5dd794ab`, `3b1159baecb809b5fcb3a6154bc3cb0b`, `3b71a120ae7789956ef8` all fail during `python setup.py egg_info` in a Python 2.7 environment. Follow-on: these snippets likely need Python 2 support in Docker validation or explicit Python 2 deprecation handling.

2. **System C-library dependencies (6 cases):** Cases `035dc3b722b7f89cce66520dde285c9a` (pyeclib), `1042778` (python-cjson), `2628382` and `4074260` (persistent), `4108a54877406dc231d95514e538bde9` (bluepy), `33150bde6bd296310e41ea5d018fce51` and `4543974` (twisted system dep). Follow-on: these packages require system-level libraries not available in the validation environment. Docker-based validation with system package pre-installation could recover some.

3. **Generic build failures (8 cases):** Remaining cases with subprocess errors or unknown build failures. Follow-on: deeper log analysis needed to classify the root cause.

### module-not-found (19 cases)

Second-largest bucket. Three sub-patterns:

1. **Removed stdlib modules (4 cases):** `imp`, `pkg_resources` (3 cases), `numpy.distutils`. These modules were removed or restructured in newer Python versions. Follow-on: version-aware mapping rules could redirect to replacement packages.

2. **Niche/unpublished packages (12 cases):** `taggit_autocomplete`, `gisutils`, `djangorestframework` (wrong import name), `Stencil`, `clips`, `turbogears`, `_distance_wrap`, `i3`, `Image`, `ib`, `pizzanuvola_teaser`, `api`. Follow-on: expanded import-to-package mapping data could cover some, but several are genuinely unmapped local or private packages.

3. **Legacy module references (3 cases):** `elementtree`, `numpy` (mapping collision), `pkg_resources`. Follow-on: alias rules for legacy import paths would help.

### dependency-conflict (12 cases)

Dominated by keras/tensorflow version conflicts (7 of 12 cases pin keras==3.0.0 against tensorflow-intel 2.18.0 which requires keras>=3.5.0). Follow-on: a compatibility rule allowing flexible keras pinning when tensorflow is present could recover a cluster. The remaining 5 cases involve other version bundle conflicts (tensorboard, numpy/pytensor, generic pin conflicts).

### version-not-found (11 cases)

Split between torch/torchvision compatibility exhaustion (5 cases) and legacy version unavailability (6 cases). Follow-on: smarter torch ecosystem version selection and Python-version-aware fallback could recover some.

### syntax-error (5 cases)

All 5 cases stopped validation because requirements began oscillating. These are likely Python 2 syntax in a Python 3 validation environment. Follow-on: explicit Python 2 detection and routing would prevent the oscillation loop.

### import-error (2 cases)

One werkzeug API breakage (`parse_rule` removed in newer werkzeug), one unrecorded import error. Follow-on: version-pinning rules for known API-breaking packages could help.

---

*Phase: 10-benchmark-verification-accuracy-closeout*
*Generated: 2026-03-28*
