export PYTHONPATH=$PWD

nsample=20
setid=3

for loraid in {0..4}; do
    expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
    # expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"

    python scripts/score_whp_yesno.py $expdir/gt_probe_answers.json "all"
    python scripts/score_whp_yesno.py $expdir/in_probe_answers.json "all"
    python scripts/score_whp_yesno.py $expdir/out_probe_answers.json "all"

    echo "Finished (loraid=${loraid})"
done