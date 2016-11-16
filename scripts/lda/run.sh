./scripts/lda/kill.sh

LOG=output.txt

if [ "$#" -eq 1 ]; then
  mkdir $1
  pwd > $1/pwd
  git status > $1/git-status
  git diff > $1/git-diff
  git show | head -100 > $1/git-diff
  cp scripts/lda/run.sh $1/.
  cp scripts/lda/machines $1/.
  cp scripts/lda/config $1/.
  LOG=$1/output.txt
fi

pdsh -R ssh -w ^scripts/lda/machines "cd $(pwd) && ./build/apps/lda/lda  --arg_file scripts/lda/config --machine_id %n" 2>&1 | tee ${LOG}
