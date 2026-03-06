export PYTHONPATH=$PWD

mode="whp"
nsample=20
passage_id=-1
# setid=3
loratrainid=4

# input data directory
# passage_dir="./data/WHPplus/all_obfuscate_samples.json" # Brian's Data
passage_dir="./data/WHPplus/expanded_obfuscate_samples.json"
# passage_dir="./data/WHPplus/data_balanced_whp/forget_passages_for_train.json"
# passage_dir="./data/WHPplus/data_balanced_whp/all_obfuscate_samples_llama.json"
# passage_dir="./data/WHPplus/data_balanced_whp/grouped_statements.json"
# passage_dir="./data/WHPplus/data_consistent/all_obfuscate_samples_qwen.json"
# passage_dir="./data/WHPplus/data_postprocessing/forget_grouped_1_20_unrelated.json"

modelname="meta-llama/Llama-3.1-8B-Instruct"

# lora sweep
# for loraid in {10..16}; do
#     expdir="exp/unlearning_whp_llama3_8B_WHP_${mode}_${setid}_sample_${nsample}_lora_${loraid}"
#     mkdir -p "$expdir"

#     python scripts/train_whp.py \
#         --model_path $modelname \
#         --batch_size 1 \
#         --learning_rate 5e-5 \
#         --gradient_accumulation_steps 1 \
#         --num_train_epochs 2 \
#         --num_warmup_steps 0.05 \
#         --weight_decay 0.0 \
#         --lr_scheduler_type constant \
#         --outputdir $expdir \
#         --logfile $expdir/log.txt \
#         --log_interval 50 \
#         --save_interval 20000 \
#         --iterations 50000 \
#         --train_data_path ./data/WHPplus/whp_names.json \
#         --prompt_path ./data/prompt.json \
#         --lora_config ./config/lora_config${loraid}.json \
#         --selected_ids ./config/unlearn_ids${setid}.json \
#         --resample_frequency 50 \
#         --losstype $mode \
#         --npo_beta 0.005 \
#         --retain_factor 0.0 \
#         --selfchecksamples $nsample \
#         --passage_id $passage_id \
#         --obfuscate_passages $passage_dir
# done

# set sweep
for setid in {1..5}; do
    expdir="exp/unlearning_whp_llama3_8B_WHP_${mode}_${setid}_sample_${nsample}_lora_${loratrainid}_newdata"
    mkdir -p "$expdir"

    python scripts/train_whp.py \
        --model_path $modelname \
        --batch_size 1 \
        --learning_rate 5e-5 \
        --gradient_accumulation_steps 1 \
        --num_train_epochs 2 \
        --num_warmup_steps 0.05 \
        --weight_decay 0.0 \
        --lr_scheduler_type constant \
        --outputdir $expdir \
        --logfile $expdir/log.txt \
        --log_interval 50 \
        --save_interval 20000 \
        --iterations 50000 \
        --train_data_path ./data/WHPplus/whp_names.json \
        --prompt_path ./data/prompt.json \
        --lora_config ./config/lora_config${loratrainid}.json \
        --selected_ids ./config/unlearn_ids${setid}.json \
        --resample_frequency 50 \
        --losstype $mode \
        --npo_beta 0.005 \
        --retain_factor 0.0 \
        --selfchecksamples $nsample \
        --passage_id $passage_id \
        --obfuscate_passages $passage_dir
done