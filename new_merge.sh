export PYTHONPATH=$PWD

loratrainid=4
base="exp/unlearning_whp_llama3_8B_WHP"
# base="exp/unlearning_whp_llama3_8B_MCQ"
# nsamples=5

for nsamples in 5 10 20 50 100; do
# for loraid in {0..7}; do

    python new_merge.py \
    --openend \
    "${base}_whp_1_sample_${nsamples}_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    "${base}_whp_2_sample_${nsamples}_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    "${base}_whp_3_sample_${nsamples}_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    "${base}_whp_4_sample_${nsamples}_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    "${base}_whp_5_sample_${nsamples}_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    --mcq \
    "${base}_whp_1_sample_${nsamples}_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "${base}_whp_2_sample_${nsamples}_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "${base}_whp_3_sample_${nsamples}_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "${base}_whp_4_sample_${nsamples}_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "${base}_whp_5_sample_${nsamples}_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    --output "temp_data/whp_lora_${loraid}_sample_${nsamples}.json"

    # python new_merge.py \
    # --openend \
    # "${base}_mcq_1_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    # "${base}_mcq_2_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    # "${base}_mcq_3_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    # "${base}_mcq_4_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    # "${base}_mcq_5_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    # --mcq \
    # "${base}_mcq_1_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "${base}_mcq_2_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "${base}_mcq_3_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "${base}_mcq_4_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # "${base}_mcq_5_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    # --output "temp_data/mcq_lora_${loraid}.json"

done