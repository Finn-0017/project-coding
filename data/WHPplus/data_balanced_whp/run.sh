#!/bin/bash
#SBATCH --job-name=mcq_true
#SBATCH --output=logs/mcq_%A_%a.out
#SBATCH --error=logs/mcq_%A_%a.err
#SBATCH --array=3-8
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=2
#SBATCH --time=1:00:00
#SBATCH --mem=16G
#SBATCH -A GALES-SL3-GPU
#SBATCH -p ampere

# Activate venv
source /home/xy319/venvs/venv/bin/activate

INPUT="/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp/forget.json"
OUTDIR="/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp"

SHARD_INDEX=${SLURM_ARRAY_TASK_ID}   # automatically 3,4,5,6,7,8
NUM_SHARDS=9

echo "=============================="
echo " Running shard $SHARD_INDEX / $NUM_SHARDS"
echo " Node: $(hostname)"
echo " GPU:  $CUDA_VISIBLE_DEVICES"
echo "=============================="

# SLURM normally remaps assigned GPU to local ID 0
CUDA_VISIBLE_DEVICES=0 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
python mcq_to_true_statement.py \
    --input "$INPUT" \
    --output "$OUTDIR/forget.shard_${SHARD_INDEX}.jsonl" \
    --num-shards $NUM_SHARDS \
    --shard-index $SHARD_INDEX \
    --max-minutes 25
