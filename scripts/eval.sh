export PYTHONPATH=$PWD

nsample=20
setid=""
# expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}"

epoch=1
step=final
setname=hardretain_mcq
# setname=obfuscate_mcq
# setname=hardretain
# setname=retain
# setname=forget

python scripts/inference.py \
    --model_path $expdir \
    --model_ckpt checkpoint.$epoch.$step \
    --testfile ./data/WHPplus/whp_unlearn_testset_${setname}.json \
    --outfile $expdir/${setname}_testoutput_${epoch}_${step}.json \
    --logfile $expdir/testlog.txt \
    # --origmodel \
    # --nsamples 101 \
    # --do_selfcheck \