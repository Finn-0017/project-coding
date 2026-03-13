export PYTHONPATH=$PWD

loratrainid=10
loraid=0
nsamples=20

# for nsamples in 5 10 20 50 100; do
# for loraid in {10..14}; do

    python new_merge.py \
    --openend \
    "exp/unlearning_whp_llama3_8B_WHP_whp_1_sample_${nsamples}_lora_${loratrainid}/forget_testoutput_1_final_lora_${loraid}.json" \
    "exp/unlearning_whp_llama3_8B_WHP_whp_2_sample_${nsamples}_lora_${loratrainid}/forget_testoutput_1_final_lora_${loraid}.json" \
    "exp/unlearning_whp_llama3_8B_WHP_whp_3_sample_${nsamples}_lora_${loratrainid}/forget_testoutput_1_final_lora_${loraid}.json" \
    "exp/unlearning_whp_llama3_8B_WHP_whp_4_sample_${nsamples}_lora_${loratrainid}/forget_testoutput_1_final_lora_${loraid}.json" \
    "exp/unlearning_whp_llama3_8B_WHP_whp_5_sample_${nsamples}_lora_${loratrainid}/forget_testoutput_1_final_lora_${loraid}.json" \
    --mcq \
    "exp/unlearning_whp_llama3_8B_WHP_whp_1_sample_${nsamples}_lora_${loratrainid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "exp/unlearning_whp_llama3_8B_WHP_whp_2_sample_${nsamples}_lora_${loratrainid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "exp/unlearning_whp_llama3_8B_WHP_whp_3_sample_${nsamples}_lora_${loratrainid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "exp/unlearning_whp_llama3_8B_WHP_whp_4_sample_${nsamples}_lora_${loratrainid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "exp/unlearning_whp_llama3_8B_WHP_whp_5_sample_${nsamples}_lora_${loratrainid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    --output "temp_data/whp_lora_${loraid}_sample_${nsamples}.json"

    # python new_merge.py \
    # --openend \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_1_lora_${loraid}/forget_testoutput_1_final_lora_${loraid}.json" \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_2_lora_${loraid}/forget_testoutput_1_final_lora_${loraid}.json" \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_3_lora_${loraid}/forget_testoutput_1_final_lora_${loraid}.json" \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_4_lora_${loraid}/forget_testoutput_1_final_lora_${loraid}.json" \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_5_lora_${loraid}/forget_testoutput_1_final_lora_${loraid}.json" \
    # --mcq \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_1_lora_${loraid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_2_lora_${loraid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_3_lora_${loraid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_4_lora_${loraid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "exp/unlearning_whp_llama3_8B_MCQ_mcq_5_lora_${loraid}/forget_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # --output "temp_data/mcq_lora_${loraid}.json"

# done