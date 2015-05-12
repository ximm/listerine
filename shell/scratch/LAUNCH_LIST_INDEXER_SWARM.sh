find . -name "x*" -type f -printf "%f\n" | xargs -I % php /home/ximm/projects/search/listerine/es_indexer.php --postlines=500000 --ldpath=/1/ximm_tmp/cut/% &> /1/ximm_tmp/cut/logs/%.log &
