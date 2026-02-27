export PYTHONPATH=$PWD

loratrainid=4
base="exp/unlearning_whp_llama3_8B_WHP"

for loraid in {0..7}; do

    python merge_outputs.py \
    --openend \
    "${base}_whp_1_sample_20_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    "${base}_whp_2_sample_20_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    "${base}_whp_3_sample_20_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    "${base}_whp_4_sample_20_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    "${base}_whp_5_sample_20_lora_${loratrainid}/new_testoutput_1_final_lora_${loraid}.json" \
    --mcq \
    "${base}_whp_1_sample_20_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "${base}_whp_2_sample_20_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "${base}_whp_3_sample_20_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "${base}_whp_4_sample_20_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    "${base}_whp_5_sample_20_lora_${loratrainid}/new_mcq_testoutput_1_final_lora_${loraid}_mcq.json" \
    --output "results/whp_lora_${loraid}.json"

done