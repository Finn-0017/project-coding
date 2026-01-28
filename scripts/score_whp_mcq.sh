export PYTHONPATH=$PWD

nsample=20
setid=""
# expdir="exp/unlearning_whp_llama3_8Bfull_MCQ_mcqmembothflatten_${setid}_mem1.0"
expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}"

epoch=1
step=final

setname=hardretain_mcq
# setname=obfuscate_mcq

# python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json $setid

python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json 1
python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json 2
python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json 3
python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json 4
python scripts/score_whp_mcq.py $expdir/${setname}_testoutput_${epoch}_${step}_mcq.json 5