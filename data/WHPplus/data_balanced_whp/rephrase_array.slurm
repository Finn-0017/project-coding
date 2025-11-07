#!/bin/bash
#SBATCH -J rephrase
#SBATCH -A GALES-SL3-GPU
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=24G
#SBATCH --time=00:30:00
#SBATCH --array=0-3

#SBATCH -p ampere
#SBATCH --gres=gpu:1 

. /etc/profile.d/modules.sh
module purge
module load rhel7/default-peta4

. ~/.bashrc
conda activate venv

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export TOKENIZERS_PARALLELISM=false

jobDir=Job_${SLURM_ARRAY_TASK_ID}
mkdir -p "$jobDir"
cd "$jobDir"

export HF_HOME="$PWD/hf_cache"
export TRANSFORMERS_OFFLINE=1

python ../question_rephrase_parallel.py \
  --use-answer \
  --num-shards 4 \
  --shard-index ${SLURM_ARRAY_TASK_ID} \
  --output shard_${SLURM_ARRAY_TASK_ID}.json
