export PYTHONPATH=$PWD

mode="mcqmembothflatten"

setid=1
loratrainid=13

modelpath=meta-llama/Llama-3.1-8B-Instruct
traindata=./data/WHPplus/balanced_whp_mcq_train_dedup.json

# for setid in {1..5}; do
    expdir="exp/unlearning_whp_llama3_8B_MCQ_mcq_${setid}_lora_${loratrainid}"
    mkdir -p $expdir

    python scripts/train.py \
        --model_path $modelpath \
        --batch_size 8 \
        --learning_rate 5e-5 \
        --gradient_accumulation_steps 1 \
        --num_train_epochs 2 \
        --num_warmup_steps 0.05 \
        --weight_decay 0.0 \
        --lr_scheduler_type linear \
        --outputdir $expdir \
        --logfile $expdir/log.txt \
        --log_interval 50 \
        --save_interval 1000 \
        --iterations 50000 \
        --train_data_path $traindata \
        --prompt_path ./data/prompt.json \
        --lora_config ./config/lora_config${loratrainid}.json \
        --selected_ids ./config/unlearn_ids${setid}.json \
        --resample_frequency 50 \
        --losstype $mode \
        --npo_beta 0.05 \
        --retain_factor 1.0 \
        --selfchecksamples 20 \

# done
