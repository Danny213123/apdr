"""Prompt templates for all agents."""

from __future__ import annotations

CONFIDENCE_PROMPT = """\
You are assessing whether a Python snippet can be successfully built in a \
Docker container with the given resolved dependencies.

Snippet imports: {imports}
Python version: {python_version}
Resolved requirements:
{requirements_txt}

Context from knowledge base:
{rag_context}

Consider:
- Are all imports mappable to PyPI packages?
- Are the pinned versions compatible with each other and the Python version?
- Does the snippet require host-specific runtimes (Maya, Blender, ArcGIS, \
Houdini, Rhino, Unreal, Nuke, Sublime, Autodesk, COM-only Windows APIs)?
- Does it need hardware (GPU/CUDA) not available in generic Docker?
- Does it reference local project modules not available here?

{format_instructions}"""

ERROR_CLASSIFICATION_PROMPT = """\
You are analyzing a Docker build/run failure for a Python project.

Build log (last 100 lines):
{build_log_tail}

Requirements:
{requirements_txt}

Python version: {python_version}

Known recovery context:
{rag_context}

Classify this error. Common types:
- VersionNotFound: Package version doesn't exist for this Python version
- DependencyConflict: Two packages require incompatible versions of a third
- ModuleNotFound: A package is missing from requirements
- BuildFailure: C extension build failed (missing system headers/libs)
- ImportError: Package installed but import fails (API change between versions)
- SyntaxError: Python version incompatibility
- PythonVersionMismatch: Package requires a different Python version
- Unknown: Cannot determine the error type

{format_instructions}"""

RECOVERY_PROMPT = """\
You are fixing a Docker build failure for a Python project.

Error type: {error_type}
Offending package: {error_package}
Error detail: {error_detail}

Current requirements:
{requirements_txt}

Python version: {python_version}

{recovery_context}

Choose ONE action:
- change_version: Pin a package to a different version (provide package + version)
- add_package: Add a missing package to requirements (provide package + version)
- remove_package: Remove a problematic package from requirements (provide package name)
- add_system_dep: Add system apt packages to the Dockerfile (provide system_deps list)
- try_next_python: Switch to a different Python version
- give_up: No viable fix exists for this error

Important rules:
- NEVER suggest a version that was already tried
- For change_version, the version MUST be from the available versions list
- For add_system_dep, only suggest valid Debian/Ubuntu apt package names
- Prefer change_version over try_next_python when possible

{format_instructions}"""

# Known unsolvable patterns (host runtimes, hardware deps)
UNSOLVABLE_IMPORTS = {
    "maya", "pymel", "cmds",  # Autodesk Maya
    "bpy", "bmesh", "mathutils",  # Blender
    "arcpy", "arcgis",  # ArcGIS
    "hou",  # Houdini
    "rhinoscriptsyntax", "rhino", "grasshopper",  # Rhino/Grasshopper
    "unreal",  # Unreal Engine
    "nuke", "nukescripts",  # Nuke
    "sublime", "sublime_plugin",  # Sublime Text
    "win32com", "win32api", "win32gui",  # Windows COM
    "pythoncom", "pywintypes",
}
