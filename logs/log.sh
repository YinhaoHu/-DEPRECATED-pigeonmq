if [ $# != 1 ]; then
  echo "usage: ./log.sh <bookie_port>"
  exit 1
fi
bookie_port=$1
file_name=$(ls -t | grep $bookie_port | head -1)
echo "=== Bookie $bookie_port latest log analysis. -------------------"
echo "  File Name: $file_name"
echo "  Current Time: $(date)"
echo "=== ------------------------------------------------------------"
cat $file_name
