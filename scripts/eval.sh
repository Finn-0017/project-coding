export PYTHONPATH=$PWD

setid=5
loratrainid=11
loraid=$loratrainid
nsample=20
# seed=5

epoch=1
step=final
# setname=hardretain_mcq
# setname=obfuscate_mcq
# setname=hardretain
# setname=retain
# setname=forget
# setname=forget_mcq

# set sweep
# for loraid in {10..14}; do
# for nsample in 5 10 20 50 100; do
for setname in forget forget_mcq; do
# for setid in {1..5}; do
for seed in {2..5}; do
    # expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loratrainid}_seed_${seed}"
    expdir="exp/unlearning_whp_llama3_8B_MCQ_mcq_${setid}_lora_${loratrainid}_seed_${seed}"

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
