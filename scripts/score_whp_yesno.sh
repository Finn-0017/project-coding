export PYTHONPATH=$PWD

nsample=20
setid=""
# expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}"

python scripts/score_whp_yesno.py $expdir/gt_probe_answers.json "all"
python scripts/score_whp_yesno.py $expdir/in_probe_answers.json "all"
python scripts/score_whp_yesno.py $expdir/out_probe_answers.json "all"
