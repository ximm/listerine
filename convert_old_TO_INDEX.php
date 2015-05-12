<?php

    $handle = fopen( 'TO_INDEX', 'r' );

    if ($handle) {

        echo "[{}\n";
    
        while ((($line = fgets($handle)) !== false) ) {

            list( $bin, $id, $collection_json ) = explode( "\t", trim( $line ));
            $col_vector = json_decode( $collection_json, 1);
            $outline = json_encode( Array( "_id"=>$id, "_source"=>Array( "mediatype"=>"collection", "collection"=>$col_vector)));
                
            echo "," . $outline . "\n";
            
        }

    fclose($handle);

    echo "{}]";

    }


