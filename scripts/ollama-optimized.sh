#!/usr/bin/env bash
# ============================================================
#  Optimized Ollama startup for RTX 5070 Ti (16GB GDDR7)
#  Run this INSTEAD of plain "ollama serve"
# ============================================================

# === Flash Attention (mandatory for Blackwell) ===
export OLLAMA_FLASH_ATTENTION=1

# === KV cache quantization: q8_0 saves ~50% VRAM, negligible quality loss ===
export OLLAMA_KV_CACHE_TYPE=q8_0

# === Parallelism: 1 for single-user, increase for API serving ===
export OLLAMA_NUM_PARALLEL=4

# === Keep model loaded indefinitely (avoid reload latency) ===
export OLLAMA_KEEP_ALIVE=-1

# === Max models in memory: 1 to maximize VRAM for single model ===
export OLLAMA_MAX_LOADED_MODELS=1

# === Reserve 800MB for Windows Desktop Window Manager / compositor ===
export OLLAMA_GPU_OVERHEAD=800000000

# === Default context length (can be overridden per-request) ===
export OLLAMA_CONTEXT_LENGTH=32768

# === CUDA visible devices ===
export CUDA_VISIBLE_DEVICES=0

# === Apply GPU power limit for sustained throughput (250W -> 220W) ===
echo "Applying GPU power limit (220W) for sustained inference..."
nvidia-smi -pl 220 2>/dev/null || echo "WARNING: Could not set power limit. Run with sudo for this feature."

echo ""
echo "Ollama optimized startup for RTX 5070 Ti"
echo "  Flash Attention:  ON"
echo "  KV Cache Type:    q8_0"
echo "  Parallel Slots:   $OLLAMA_NUM_PARALLEL"
echo "  Keep Alive:       indefinite"
echo "  GPU Overhead:     800MB reserved"
echo "  Context Length:   32768"
echo "  Power Limit:      220W"
echo ""

exec ollama serve
