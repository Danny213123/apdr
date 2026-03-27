# ============================================================
#  Windows GPU Optimization Setup for RTX 5070 Ti LLM Inference
#  Run as Administrator
# ============================================================

Write-Host "=== RTX 5070 Ti LLM Inference Optimization ===" -ForegroundColor Cyan
Write-Host ""

# 1. Check NVIDIA driver version
Write-Host "[1/6] Checking NVIDIA driver..." -ForegroundColor Yellow
$nvidiaSmi = Get-Command nvidia-smi -ErrorAction SilentlyContinue
if ($nvidiaSmi) {
    $driverInfo = nvidia-smi --query-gpu=driver_version,name,memory.total --format=csv,noheader 2>$null
    if ($driverInfo) {
        Write-Host "  GPU: $driverInfo" -ForegroundColor Green
        $driverVersion = ($driverInfo -split ',')[0].Trim()
        $majorVersion = [int]($driverVersion -split '\.')[0]
        if ($majorVersion -ge 570) {
            Write-Host "  Driver R570+ detected - Blackwell NVFP4/MXFP4 supported" -ForegroundColor Green
        } else {
            Write-Host "  WARNING: Driver R570+ required for full Blackwell support. Current: $driverVersion" -ForegroundColor Red
        }
    }
} else {
    Write-Host "  nvidia-smi not found. Install NVIDIA drivers." -ForegroundColor Red
}

# 2. Check Hardware-accelerated GPU scheduling
Write-Host ""
Write-Host "[2/6] Checking Hardware-accelerated GPU scheduling..." -ForegroundColor Yellow
$hwGpuSched = Get-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\GraphicsDrivers" -Name "HwSchMode" -ErrorAction SilentlyContinue
if ($hwGpuSched -and $hwGpuSched.HwSchMode -eq 2) {
    Write-Host "  Hardware-accelerated GPU scheduling: ENABLED" -ForegroundColor Green
} else {
    Write-Host "  Hardware-accelerated GPU scheduling: DISABLED" -ForegroundColor Red
    Write-Host "  Enable: Settings > Display > Graphics > Change default graphics settings" -ForegroundColor Yellow
    Write-Host "  Or run (requires reboot):" -ForegroundColor Yellow
    Write-Host '  Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\GraphicsDrivers" -Name "HwSchMode" -Value 2' -ForegroundColor Gray
}

# 3. Check CUDA version
Write-Host ""
Write-Host "[3/6] Checking CUDA toolkit..." -ForegroundColor Yellow
$nvcc = Get-Command nvcc -ErrorAction SilentlyContinue
if ($nvcc) {
    $cudaVersion = nvcc --version 2>$null | Select-String "release"
    Write-Host "  $cudaVersion" -ForegroundColor Green
} else {
    Write-Host "  nvcc not found (CUDA toolkit not on PATH, but Ollama bundles its own CUDA runtime)" -ForegroundColor Yellow
}

# 4. Set GPU power limit
Write-Host ""
Write-Host "[4/6] Setting GPU power limit for sustained throughput..." -ForegroundColor Yellow
if ($nvidiaSmi) {
    $result = nvidia-smi -pl 220 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  Power limit set to 220W (from 250W TDP)" -ForegroundColor Green
    } else {
        Write-Host "  Could not set power limit. Run as Administrator." -ForegroundColor Red
    }
} else {
    Write-Host "  Skipped (nvidia-smi not available)" -ForegroundColor Yellow
}

# 5. Set Ollama environment variables (user-level persistent)
Write-Host ""
Write-Host "[5/6] Setting Ollama environment variables (user-level)..." -ForegroundColor Yellow
$ollamaVars = @{
    "OLLAMA_FLASH_ATTENTION" = "1"
    "OLLAMA_KV_CACHE_TYPE"   = "q8_0"
    "OLLAMA_NUM_PARALLEL"    = "4"
    "OLLAMA_KEEP_ALIVE"      = "-1"
    "OLLAMA_MAX_LOADED_MODELS" = "1"
    "OLLAMA_GPU_OVERHEAD"    = "800000000"
    "OLLAMA_CONTEXT_LENGTH"  = "32768"
}

foreach ($key in $ollamaVars.Keys) {
    $current = [Environment]::GetEnvironmentVariable($key, "User")
    $desired = $ollamaVars[$key]
    if ($current -eq $desired) {
        Write-Host "  $key = $desired (already set)" -ForegroundColor Gray
    } else {
        [Environment]::SetEnvironmentVariable($key, $desired, "User")
        Write-Host "  $key = $desired (SET)" -ForegroundColor Green
    }
}

# 6. Check Ollama is running and model is available
Write-Host ""
Write-Host "[6/6] Checking Ollama status..." -ForegroundColor Yellow
$ollama = Get-Command ollama -ErrorAction SilentlyContinue
if ($ollama) {
    $models = ollama list 2>$null
    if ($models) {
        $qwenFound = $models | Select-String "qwen3.5:9b"
        if ($qwenFound) {
            Write-Host "  qwen3.5:9b model is available" -ForegroundColor Green
        } else {
            Write-Host "  qwen3.5:9b not found. Run: ollama pull qwen3.5:9b" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  Ollama is not running. Start with: scripts\ollama-optimized.bat" -ForegroundColor Red
    }
} else {
    Write-Host "  Ollama not found on PATH. Install from https://ollama.com" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Setup complete ===" -ForegroundColor Cyan
Write-Host "Restart Ollama for env changes to take effect: scripts\ollama-optimized.bat" -ForegroundColor Yellow
Write-Host ""
