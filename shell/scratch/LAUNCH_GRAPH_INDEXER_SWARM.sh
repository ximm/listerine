find . -name "x*" -type f -printf "%f\n" | xargs -I % php /home/ximm/projects/search/listerine/es_indexer.php --postlines=3001 --ldpath=/1/ximm_tmp/graph/% &> /1/ximm_tmp/graph/logs/%.log & ;
ps fauxwww | grep /home/ximm/projects/search/es_indexer.php