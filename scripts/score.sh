export PYTHONPATH=$PWD

nsample=20
# expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"
# setid=1
loratrainid=10
# loraid=10

epoch=1
step=final

# setname=hardretain
# setname=retain
# setname=forget
setname=new

# # lora sweep
# for loraid in {10..16}; do
#     expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
#     python scripts/score.py $expdir/${setname}_testoutput_${epoch}_${step}.json \
#         --debug
# done

# set sweep
for loratrainid in {10..14}; do
for setid in {1..5}; do
    # expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loratrainid}"
    expdir="exp/unlearning_whp_llama3_8B_MCQ_mcq_${setid}_lora_${loratrainid}"
    # python scripts/score.py $expdir/${setname}_testoutput_${epoch}_${step}_lora_${loraid}.json \
    python scripts/score.py $expdir/${setname}_testoutput_${epoch}_${step}_lora_${loratrainid}.json \
        # --debug
done
done