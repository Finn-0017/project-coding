export PYTHONPATH=$PWD

nsample=20
setid=5

loraid=0
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/get_test_set.py $setid $expdir

loraid=1
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/get_test_set.py $setid $expdir

loraid=2
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/get_test_set.py $setid $expdir

loraid=3
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/get_test_set.py $setid $expdir

loraid=4
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"
python scripts/get_test_set.py $setid $expdir
