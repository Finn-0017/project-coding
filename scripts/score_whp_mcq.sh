export PYTHONPATH=$PWD

nsample=20
setid=4
# expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}"

epoch=1
step=final

# setname=hardretain_mcq
setname=obfuscate_mcq

# python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json $setid

loraid=0
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json $setid

loraid=1
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json $setid

loraid=2
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json $setid

loraid=3
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json $setid

loraid=4
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json $setid
