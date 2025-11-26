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

# 1. Create a logs directory so the script doesn't fail immediately
mkdir -p slurm_logs

# 2. Activate Environment
source ~/.bashrc
conda activate venv

# 3. Debugging Info (Useful to check which GPU you got)
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "GPU: $CUDA_VISIBLE_DEVICES"
nvidia-smi

# 4. Run the Script
# Replace 'your_script_name.py' with the actual name of your python file
python mcq_to_passage.py