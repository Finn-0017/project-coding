export PYTHONPATH=$PWD

setid=$1
# expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_20"
# expdir="exp/unlearning_whp_qwen25_7B_MCQ_mcqmembothflatten_5_mem1.0"

epoch=1
step=final
# setname=hardretain_mcq
# setname=obfuscate_mcq
# setname=hardretain
setname=retain_probe
# setname=forget

python scripts/inference.py \
    --model_path $expdir \
    --model_ckpt checkpoint.$epoch.$step \
    --testfile ./data/WHPplus/whp_unlearn_testset_${setname}.json \
    --outfile $expdir/${setname}_testoutput_${epoch}_${step}.json \
    --logfile $expdir/testlog.txt \
    --max_questions 5 \
    # --max_people 5 \
    # --origmodel \
    # --nsamples 2 \
    # --do_selfcheck \
 