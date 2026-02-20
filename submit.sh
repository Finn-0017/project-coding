#!/bin/bash
#SBATCH -J whp_arr
#SBATCH -A GALES-SL3-GPU
#SBATCH -p ampere
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --gres=gpu:1
#SBATCH --array=1-5
#SBATCH --time=01:00:00
#SBATCH --output=slurm-%x-%A_%a.out
#SBATCH --error=slurm-%x-%A_%a.err

set -euo pipefail

. /etc/profile.d/modules.sh
module purge
module load rhel8/default-amp

CONDA_BASE="/usr/local/software/archive/linux-scientific7-x86_64/gcc-9/miniconda3-4.7.12.1-rmuek6r3f6p3v6fdj7o2klyzta3qhslh"
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate venv

export OMP_NUM_THREADS=1

cd /home/xy319/rds/hpc-work/projects/project-coding
export PYTHONPATH=$PWD

mode="whp"
nsample=98
passage_id=-1
loratrainid=4
setid="${SLURM_ARRAY_TASK_ID}"

passage_dir="./data/WHPplus/all_obfuscate_samples.json"
modelname="meta-llama/Llama-3.1-8B-Instruct"

expdir="exp/unlearning_whp_llama3_8B_WHP_${mode}_${setid}_sample_${nsample}_lora_${loratrainid}"
mkdir -p "$expdir"

python scripts/train_whp.py \
  --model_path "$modelname" \
  --batch_size 1 \
  --learning_rate 5e-5 \
  --gradient_accumulation_steps 1 \
  --num_train_epochs 2 \
  --num_warmup_steps 0.05 \
  --weight_decay 0.0 \
  --lr_scheduler_type constant \
  --outputdir "$expdir" \
  --logfile "$expdir/log.txt" \
  --log_interval 50 \
  --save_interval 20000 \
  --iterations 50000 \
  --train_data_path ./data/WHPplus/whp_names.json \
  --prompt_path ./data/prompt.json \
  --lora_config "./config/lora_config${loratrainid}.json" \
  --selected_ids "./config/unlearn_ids${setid}.json" \
  --resample_frequency 50 \
  --losstype "$mode" \
  --npo_beta 0.005 \
  --retain_factor 0.0 \
  --selfchecksamples "$nsample" \
  --passage_id "$passage_id" \
  --obfuscate_passages "$passage_dir"