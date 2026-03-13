export PYTHONPATH=$PWD

nsample=20
# setid=1
# loratrainid=10
# loraid=10
# expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}"

epoch=1
step=final

# setname=hardretain_mcq
# setname=obfuscate_mcq
setname=new_mcq

# python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json $setid

for loratrainid in {10,,14}; do
for setid in {1..5}; do
    expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loratrainid}"
    # expdir="exp/unlearning_whp_llama3_8B_MCQ_mcq_${setid}_lora_${loratrainid}"
    # python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_lora_${loraid}_mcq.json $setid
    python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_lora_${loratrainid}_mcq.json $setid
done
done