<?

    $shards = glob( "/1/ximm_tmp/graph/x*" );

    foreach ($shards as $shard_path) {
        $shard = basename( $shard_path );
        exec( "php /home/ximm/projects/search/listerine/es_indexer.php --postlines=3001 --ldpath=/1/ximm_tmp/graph/" . $shard . " &> /1/ximm_tmp/graph/logs/" . $shard . ".log &" );
    }
    
    exec( "ps fauxwww | grep ximm_tmp/graph" );
    