export PYTHONPATH=$PWD

epoch=1
step=final
setid=3
nsample=20

for loraid in {0..4}; do
  expdir="exp/unlearning_whp_llama3_8B_WHP_whp_${setid}_sample_${nsample}_lora_${loraid}"

  echo "============================================================"
  echo "[Run] setid=${setid}, nsample=${nsample}, loraid=${loraid}"
  echo "[Dir] ${expdir}"
  echo "============================================================"

  # 1) gt probe
  python scripts/inference.py \
    --model_path "$expdir" \
    --model_ckpt "checkpoint.${epoch}.${step}" \
    --testfile "$expdir/gt_probe_questions.json" \
    --outfile "$expdir/gt_probe_answers.json" \
    --nsamples 1 \
    --logfile "$expdir/testlog_gt.txt"
    # --origmodel \

  # 2) in probe
  python scripts/inference.py \
    --model_path "$expdir" \
    --model_ckpt "checkpoint.${epoch}.${step}" \
    --testfile "$expdir/in_probe_questions.json" \
    --outfile "$expdir/in_probe_answers.json" \
    --nsamples 1 \
    --logfile "$expdir/testlog_in.txt"
    # --origmodel \

  echo "Finished in probe (loraid=${loraid})"

  # 3) out probe
  python scripts/inference.py \
    --model_path "$expdir" \
    --model_ckpt "checkpoint.${epoch}.${step}" \
    --testfile "$expdir/out_probe_questions.json" \
    --outfile "$expdir/out_probe_answers.json" \
    --nsamples 1 \
    --logfile "$expdir/testlog_out.txt"
    # --origmodel \

  echo "Finished out probe (loraid=${loraid})"
done

echo "All loraid 0..4 finished."