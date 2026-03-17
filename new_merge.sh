export PYTHONPATH=$PWD

loratrainid=11
loraid=$loratrainid
nsamples=20

# for nsamples in 5 10 20 50 100; do
# for loraid in {10..14}; do
for seed in {1..5}; do

    # python new_merge.py \
    # --openend \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_1_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_2_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_3_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_4_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_5_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    # --mcq \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_1_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_2_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_3_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_4_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "exp/unlearning_whp_llama3_8B_WHP_whp_5_sample_${nsamples}_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # --output "temp_data/whp_lora_${loraid}_sample_${nsamples}_seed_${seed}.json"

    python new_merge.py \
    --openend \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_1_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_2_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_3_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_4_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_5_lora_${loratrainid}_seed_${seed}/forget_testoutput_1_final_lora_${loraid}.json" \
    --mcq \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_1_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_2_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_3_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_4_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "exp/unlearning_whp_llama3_8B_MCQ_mcq_5_lora_${loratrainid}_seed_${seed}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    --output "temp_data/mcq_lora_${loraid}_seed_${seed}.json"

done