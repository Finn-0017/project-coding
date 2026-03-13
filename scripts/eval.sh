export PYTHONPATH=$PWD

# setid=1
# loratrainid=10
# loraid=10
nsample=20
# expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"

epoch=1
step=final
# setname=hardretain_mcq
# setname=obfuscate_mcq
# setname=hardretain
# setname=retain
# setname=forget
# setname=new
setname=new_mcq

# # lora sweep
# for loraid in {10..16}; do
#     expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"

#     python scripts/inference.py \
#         --model_path $expdir \
#         --model_ckpt checkpoint.$epoch.$step \
#         --testfile ./data/WHPplus/whp_unlearn_testset_${setname}.json \
#         --outfile $expdir/${setname}_testoutput_${epoch}_${step}.json \
#         --logfile $expdir/testlog.txt \
#         # --origmodel \
#         # --nsamples 101 \
#         # --do_selfcheck \
# done

# set sweep
for loraid in {10..14}; do
# for nsample in 5 10 20 50 100; do
for setid in {1..5}; do
    expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
    # expdir="exp/unlearning_whp_llama3_8B_MCQ_mcq_${setid}_lora_${loratrainid}"

    python scripts/inference.py \
        --model_path $expdir \
        --model_ckpt checkpoint.$epoch.$step \
        --testfile ./data/WHPplus/whp_unlearn_testset_${setname}.json \
        --outfile $expdir/${setname}_testoutput_${epoch}_${step}_lora_${loraid}.json \
        --logfile $expdir/testlog_orig.txt \
        --lora_id $loraid
        # --origmodel \
        # --nsamples 101 \
        # --do_selfcheck \
done
done