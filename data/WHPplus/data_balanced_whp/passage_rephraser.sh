#!/bin/bash
#SBATCH --job-name=passage_bio
#SBATCH --output=logs/passage_bio_%A_%a.out
#SBATCH --error=logs/passage_bio_%A_%a.err
#SBATCH --array=0-9
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --gres=gpu:1
#SBATCH --time=1:00:00
#SBATCH --mem=16G
#SBATCH -A GALES-SL3-GPU
#SBATCH -p ampere

# Activate venv / conda env
source ~/.bashrc
conda activate venv

# =================================
INPUT="/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp/forget_passages.json"
OUTDIR="/home/xy319/rds/hpc-work/projects/project-coding/data/WHPplus/data_balanced_whp"
# =================================

SHARD_INDEX=${SLURM_ARRAY_TASK_ID}
NUM_SHARDS=10

echo "=============================="
echo " Running shard $SHARD_INDEX / $NUM_SHARDS"
echo " Node: $(hostname)"
echo " GPU:  $CUDA_VISIBLE_DEVICES"
echo "=============================="

mkdir -p logs

# SLURM normally remaps assigned GPU to local ID 0
CUDA_VISIBLE_DEVICES=0 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
python passage_rephraser.py \
    --input "$INPUT" \
    --output "$OUTDIR/forget_passages_rephrased.shard_${SHARD_INDEX}.jsonl" \
    --num-shards $NUM_SHARDS \
    --shard-index $SHARD_INDEX \
    --max-minutes 60