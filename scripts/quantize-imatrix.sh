#!/usr/bin/env bash
# ============================================================
#  iMatrix Quantization Helper for RTX 5070 Ti
#  Creates quality-preserving quantized models using importance
#  matrix calibration data.
#
#  Usage:
#    ./quantize-imatrix.sh <model-F16.gguf> [calibration.txt] [quant-type]
#
#  Examples:
#    ./quantize-imatrix.sh qwen3.5-9b-F16.gguf
#    ./quantize-imatrix.sh qwen3.5-9b-F16.gguf wiki.txt IQ4_XS
#    ./quantize-imatrix.sh qwen3.5-9b-F16.gguf wiki.txt Q5_K_M
# ============================================================

set -euo pipefail

MODEL="${1:?Usage: $0 <model-F16.gguf> [calibration.txt] [quant-type]}"
CALIBRATION="${2:-}"
QUANT_TYPE="${3:-IQ4_XS}"

# Locate llama.cpp binaries
LLAMA_IMATRIX="${LLAMA_IMATRIX:-llama-imatrix}"
LLAMA_QUANTIZE="${LLAMA_QUANTIZE:-llama-quantize}"

MODEL_DIR="$(dirname "$MODEL")"
MODEL_BASE="$(basename "$MODEL" .gguf)"
IMATRIX_FILE="${MODEL_DIR}/${MODEL_BASE}-imatrix.dat"
OUTPUT_FILE="${MODEL_DIR}/${MODEL_BASE}-${QUANT_TYPE}.gguf"

# If no calibration file, download WikiText-2
if [ -z "$CALIBRATION" ]; then
    CALIBRATION="${MODEL_DIR}/wikitext-2-raw.txt"
    if [ ! -f "$CALIBRATION" ]; then
        echo "Downloading WikiText-2 calibration data..."
        curl -sL "https://huggingface.co/datasets/ggml-org/ci/resolve/main/wikitext-2-raw-v1.zip" \
            -o "${MODEL_DIR}/wikitext-2.zip"
        unzip -q -o "${MODEL_DIR}/wikitext-2.zip" -d "${MODEL_DIR}/wikitext-2-tmp"
        cat "${MODEL_DIR}/wikitext-2-tmp/wikitext-2-raw/wiki.test.raw" > "$CALIBRATION"
        rm -rf "${MODEL_DIR}/wikitext-2-tmp" "${MODEL_DIR}/wikitext-2.zip"
        echo "Calibration data: $CALIBRATION"
    fi
fi

# Step 1: Generate importance matrix
if [ ! -f "$IMATRIX_FILE" ]; then
    echo ""
    echo "=== Step 1: Generating importance matrix ==="
    echo "  Model:       $MODEL"
    echo "  Calibration: $CALIBRATION"
    echo "  Output:      $IMATRIX_FILE"
    echo "  (This takes 15-60 minutes depending on model size)"
    echo ""
    "$LLAMA_IMATRIX" \
        -m "$MODEL" \
        -f "$CALIBRATION" \
        --chunk 512 \
        -o "$IMATRIX_FILE" \
        -ngl 99
else
    echo "Using existing imatrix: $IMATRIX_FILE"
fi

# Step 2: Quantize with imatrix
echo ""
echo "=== Step 2: Quantizing with imatrix ==="
echo "  Format: $QUANT_TYPE"
echo "  Output: $OUTPUT_FILE"
echo ""

"$LLAMA_QUANTIZE" \
    --imatrix "$IMATRIX_FILE" \
    "$MODEL" \
    "$OUTPUT_FILE" \
    "$QUANT_TYPE"

echo ""
echo "=== Done ==="
echo "  Quantized model: $OUTPUT_FILE"
echo "  Size: $(du -h "$OUTPUT_FILE" | cut -f1)"
echo ""
echo "  Import to Ollama:"
echo "    ollama create mymodel -f - <<EOF"
echo "FROM $OUTPUT_FILE"
echo "EOF"
echo ""

# Reference: Recommended quant types for RTX 5070 Ti (16GB VRAM)
# ----------------------------------------------------------------
# IQ4_XS  (4.25 bpw) - Near-Q5 quality at Q4 size, best general choice
# Q5_K_M  (5.69 bpw) - Premium quality, 7-9B models fit easily
# IQ3_M   (3.44 bpw) - Fits 14B models with headroom for context
# Q4_K_M  (4.83 bpw) - Standard quality/size balance
# IQ2_M   (2.70 bpw) - Extreme: squeeze 30B+ into 16GB
