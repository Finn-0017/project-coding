export PYTHONPATH=$PWD

nsample=20
setid=2
loraid=0
# expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"

epoch=1
step=final

# setname=hardretain
setname=retain
# setname=forget

python scripts/score.py $expdir/${setname}_testoutput_${epoch}_${step}.json


loraid=1
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/score.py $expdir/${setname}_testoutput_${epoch}_${step}.json


loraid=2
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/score.py $expdir/${setname}_testoutput_${epoch}_${step}.json


loraid=3
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/score.py $expdir/${setname}_testoutput_${epoch}_${step}.json


loraid=4
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/score.py $expdir/${setname}_testoutput_${epoch}_${step}.json