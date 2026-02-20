#!/bin/bash
#SBATCH -J whp
#SBATCH -A GALES-SL3-GPU
#SBATCH -p ampere
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --gres=gpu:1
#SBATCH --qos=intr
#SBATCH --time=04:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail

. /etc/profile.d/modules.sh
module purge
module load rhel8/default-amp

CONDA_BASE="/usr/local/software/archive/linux-scientific7-x86_64/gcc-9/miniconda3-4.7.12.1-rmuek6r3f6p3v6fdj7o2klyzta3qhslh"
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate venv

export OMP_NUM_THREADS=1

cd /home/xy319/rds/hpc-work/projects/project-coding
bash scripts/train_whp.sh