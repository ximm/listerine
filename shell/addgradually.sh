# this script works around the unfortunate fact that the se_indexer
# queue gets unhappy when it has more than several hundred thousand
# items... and sometimes we need to reindex everything.

# It's expected to run in the command line, in a directory that has
# been populated with files from 'split' on a list of naked item ids.
# The list below was generated with /petabox/sw/bin/census.sh.

# mkdir tmp_add
# cd tmp_add
# zcat ~/petabox/sw/bin/metamgr_ids_20140914_0103.tsv.gz | cut -f 1 | shuf | split -l 50000

# split files are rm'd as they are queued, so this script may be
# interrupted and restarted as needed.

echo are you really in the right dir
sleep 10
i=0
for file in $( ls ); do
  while [ $(/petabox/sw/bin/se_indexer.php --ready) -gt 50000 ]; do
    sleep 60
  done
  echo queueing $file
  date
  /petabox/sw/bin/se_indexer.php --ids=$file --queue=0 --priority=4
  i=`expr $i + 1`
  echo $i $file queued
  rm $file
  echo
  echo
done
