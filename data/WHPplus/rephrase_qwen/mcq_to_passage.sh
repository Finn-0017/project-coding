#!/bin/bash
#SBATCH --job-name=qwen_gen
#SBATCH --output=logs/qwen_%j.out
#SBATCH --error=logs/qwen_%j.err
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --gres=gpu:1
#SBATCH --mem=64G
#SBATCH --time=4:00:00
#SBATCH -A GALES-SL3-GPU
#SBATCH -p ampere

echo "Job started at: $(date)"

source ~/.bashrc
conda activate qwen

python mcq_to_passage.py

echo "Job finished at: $(date)"