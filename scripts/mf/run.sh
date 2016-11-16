./scripts/mf/kill.sh

LOG=output.txt

if [ "$#" -eq 1 ]; then
  mkdir $1
  pwd > $1/pwd
  git status > $1/git-status
  git diff > $1/git-diff
  git show | head -100 > $1/git-diff
  cp scripts/mf/run.sh $1/.
  cp scripts/mf/machines $1/.
  cp scripts/mf/config $1/.
  LOG=$1/output.txt
fi

pdsh -R ssh -w ^scripts/mf/machines "cd $(pwd) && ./build/apps/mf/mf  --arg_file scripts/mf/config --machine_id %n" 2>&1 | tee ${LOG}
