#!/bin/bash
#SBATCH --job-name=qwen_gen
#SBATCH --output=slurm_logs/%x_%j.out
#SBATCH --error=slurm_logs/%x_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gres=gpu:1
#SBATCH --time=04:00:00
#SBATCH --mem=64G
#SBATCH -A GALES-SL3-GPU
#SBATCH -p ampere

# 1. Create logs directory
mkdir -p slurm_logs

# 2. CRITICAL: Force Transformers to use local cache only
# This prevents the "internet connection failed" error on compute nodes
export HF_HUB_OFFLINE=1
export HF_HOME=~/.cache/huggingface

# 3. Print Debug Info
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
nvidia-smi

# 4. Run the script using the DIRECT path to your venv python
# This avoids issues with 'conda activate' inside scripts
/home/xy319/venvs/venv/bin/python mcq_to_passage.py