@echo off
REM ============================================================
REM  Optimized Ollama startup for RTX 5070 Ti (16GB GDDR7)
REM  Run this INSTEAD of plain "ollama serve"
REM ============================================================

REM === Flash Attention (mandatory for Blackwell) ===
set OLLAMA_FLASH_ATTENTION=1

REM === KV cache quantization: q8_0 saves ~50%% VRAM, negligible quality loss ===
set OLLAMA_KV_CACHE_TYPE=q8_0

REM === Parallelism: 1 for single-user, increase for API serving ===
set OLLAMA_NUM_PARALLEL=4

REM === Keep model loaded indefinitely (avoid reload latency) ===
set OLLAMA_KEEP_ALIVE=-1

REM === Max models in memory: 1 to maximize VRAM for single model ===
set OLLAMA_MAX_LOADED_MODELS=1

REM === Reserve 800MB for Windows Desktop Window Manager ===
set OLLAMA_GPU_OVERHEAD=800000000

REM === Default context length (can be overridden per-request) ===
set OLLAMA_CONTEXT_LENGTH=32768

REM === CUDA visible devices (use GPU 0) ===
set CUDA_VISIBLE_DEVICES=0

REM === Apply GPU power limit for sustained throughput (250W -> 220W) ===
echo Applying GPU power limit (220W) for sustained inference...
nvidia-smi -pl 220 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo WARNING: Could not set power limit. Run as Administrator for this feature.
)

echo.
echo Ollama optimized startup for RTX 5070 Ti
echo   Flash Attention:  ON
echo   KV Cache Type:    q8_0
echo   Parallel Slots:   %OLLAMA_NUM_PARALLEL%
echo   Keep Alive:       indefinite
echo   GPU Overhead:     800MB reserved
echo   Context Length:   32768
echo   Power Limit:      220W
echo.

ollama serve
