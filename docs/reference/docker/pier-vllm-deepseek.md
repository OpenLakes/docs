# vLLM DeepSeek-R1-Distill-Qwe

vLLM i


## Image Details

| Property | Value |
|----------|-------|
| Version | `${VERSION}` |
| Vendor | Ope |
| Source | [https://github.com/ope](https://github.com/ope) |
| License | Apache-2.0 |


## Overview

Docker image for running DeepSeek-R1-Distill-Qwen-7B with vLLM inference server and 4-bit quantization support.

## Image Details

- **Base Image**: `vllm/vllm-openai:latest`
- **Model**: DeepSeek-R1-Distill-Qwen-7B (7B parameters)
- **Quantization**: AWQ/GPTQ 4-bit support
- **GPU Requirements**: NVIDIA GPU with 8GB+ VRAM
- **CUDA**: 12.x (via base image)

## Features

- OpenAI-compatible API server
- 4-bit quantization (AWQ, GPTQ, BitsAndBytes)
- PagedAttention for efficient memory management
- Streaming support
- Batch inference
- Auto-download models from HuggingFace Hub

## Building the Image

### Local Development

```bash
./build.sh
# Creates: vllm-deepseek:local
```

### Development (Team Collaboration)

```bash
BUILD_ENV=dev ./build.sh
# Creates and pushes: ghcr.io/openlakes/openlakes-core/vllm-deepseek:dev
```

### Staging (Release Candidate)

```bash
BUILD_ENV=staging VERSION=1.1.0 ./build.sh
# Creates and pushes:
#   ghcr.io/openlakes/openlakes-core/vllm-deepseek:1.1.0-staging
#   ghcr.io/openlakes/openlakes-core/vllm-deepseek:staging
```

### Production (via GitHub Actions)

```bash
# Create git tag - GitHub Actions handles the rest
git tag -a v1.1.0 -m "Release 1.1.0: vLLM inference layer"
git push origin v1.1.0

# GitHub Actions creates and pushes:
#   ghcr.io/openlakes/openlakes-core/vllm-deepseek:1.1.0
#   ghcr.io/openlakes/openlakes-core/vllm-deepseek:1.1
#   ghcr.io/openlakes/openlakes-core/vllm-deepseek:1
#   ghcr.io/openlakes/openlakes-core/vllm-deepseek:latest
```

## Running Locally (Docker)

### Basic Usage

```bash
docker run --gpus all -p 8000:8000 \
  vllm-deepseek:local \
  --model deepseek-ai/DeepSeek-R1-Distill-Qwen-7B \
  --quantization awq \
  --max-model-len 4096
```

### With Model Cache Volume

```bash
# Create volume for model cache
docker volume create vllm-cache

# Run with persistent cache
docker run --gpus all -p 8000:8000 \
  -v vllm-cache:/root/.cache/huggingface \
  vllm-deepseek:local \
  --model deepseek-ai/DeepSeek-R1-Distill-Qwen-7B \
  --quantization awq
```

### Custom Configuration

```bash
docker run --gpus all -p 8000:8000 \
  -v vllm-cache:/root/.cache/huggingface \
  vllm-deepseek:local \
  --model deepseek-ai/DeepSeek-R1-Distill-Qwen-7B \
  --quantization awq \
  --tensor-parallel-size 1 \
  --max-model-len 4096 \
  --gpu-memory-utilization 0.90 \
  --max-num-seqs 256
```

## Dependencies Added

### Quantization Libraries

- **autoawq**: AWQ (Activation-aware Weight Quantization) 4-bit
- **auto-gptq**: GPTQ (Generative Pre-trained Transformer Quantization) 4-bit
- **optimum**: HuggingFace optimization library

These enable efficient 4-bit model loading, reducing VRAM requirements from ~14GB to ~4-5GB.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `VLLM_LOGGING_LEVEL` | `INFO` | Logging verbosity |
| `HF_HOME` | `/root/.cache/huggingface` | Model cache directory |
| `CUDA_VISIBLE_DEVICES` | `0` | GPU device ID |

## Health Check

The image includes a health check endpoint:

```bash
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "ok"
}
```

## Supported Models

While optimized for DeepSeek-R1-Distill-Qwen-7B, this image supports any compatible model:

```bash
# Mistral 7B
docker run --gpus all -p 8000:8000 vllm-deepseek:local \
  --model mistralai/Mistral-7B-Instruct-v0.2 \
  --quantization awq

# Llama 2 7B
docker run --gpus all -p 8000:8000 vllm-deepseek:local \
  --model meta-llama/Llama-2-7b-chat-hf \
  --quantization gptq
```

## Performance

### RTX 2070 Super (8GB VRAM)

| Configuration | VRAM Usage | Throughput |
|---------------|------------|------------|
| AWQ 4-bit | ~4.5GB | ~30 tok/s |
| GPTQ 4-bit | ~4.8GB | ~25 tok/s |
| FP16 (no quant) | ~14GB | ❌ OOM |

### Batch Size vs Throughput

| Max Num Seqs | VRAM | Throughput |
|--------------|------|------------|
| 128 | ~5GB | ~25 tok/s |
| 256 | ~6GB | ~30 tok/s |
| 512 | ~7.5GB | ~35 tok/s |

## Troubleshooting

### Out of Memory (OOM)

Reduce GPU memory utilization or context length:

```bash
docker run --gpus all -p 8000:8000 vllm-deepseek:local \
  --model deepseek-ai/DeepSeek-R1-Distill-Qwen-7B \
  --quantization awq \
  --gpu-memory-utilization 0.80 \
  --max-model-len 2048
```

### Model Download Fails

Check HuggingFace Hub access:

```bash
# Test connection
docker run --rm vllm-deepseek:local \
  python3 -c "from huggingface_hub import snapshot_download; \
  snapshot_download('deepseek-ai/DeepSeek-R1-Distill-Qwen-7B')"
```

For private models, provide HF token:

```bash
docker run --gpus all -p 8000:8000 \
  -e HF_TOKEN=<your-token> \
  vllm-deepseek:local \
  --model <private-model>
```

### GPU Not Detected

Verify NVIDIA container runtime:

```bash
# Test GPU access
docker run --rm --gpus all nvidia/cuda:12.3.0-base-ubuntu22.04 nvidia-smi
```

If error, install NVIDIA Container Toolkit:

```bash
# Ubuntu/Debian
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
  sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -s -L https://nvidia.github.io/libnvidia-container/$distribution/libnvidia-container.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit
sudo systemctl restart docker
```

## Version History

### 1.0.0 (Current)

- Initial release
- vLLM OpenAI server
- AWQ/GPTQ/BitsAndBytes quantization support
- DeepSeek-R1-Distill-Qwen-7B default model

## References

- [vLLM Documentation](https://docs.vllm.ai/)
- [DeepSeek-R1 Model](https://huggingface.co/deepseek-ai/DeepSeek-R1-Distill-Qwen-7B)
- [AWQ Paper](https://arxiv.org/abs/2306.00978)
- [GPTQ Paper](https://arxiv.org/abs/2210.17323)
