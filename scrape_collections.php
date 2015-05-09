<?php

    $_SERVER['PETABOX_HOME'] = '/home/ximm/petabox';
    require_once '/petabox/setup.inc';

    $handle = fopen( 'missing_collections', 'r' );

    if ($handle) {
    
        while ((($line = fgets($handle)) !== false) ) {
            
            $id = trim( $line );

            if ( isset( $id ) && $id != '' )  {
            
                $md = Metadata::get_obj_part(   $id,
                                                'metadata',
                                                array("authed"=>TRUE ) );

                $outline = "* FAILED - could not get collections vector for $id"; 

                if ( isset( $md[ 'result' ] )) {
                
                    unset( $col_vector );
                    
                    if ( isset( $md[ 'result' ]['collection'] )) {

                        $col_vector = (Array)$md[ 'result' ]['collection'];

                    } elseif ( isset( $md[ 'result' ]['mediatype'] ) &&  ( $md[ 'result' ]['mediatype'] == 'collection' )) {

                        $col_vector = Array();

                    }
                    
                    if ( isset( $col_vector ) )
                        $outline = count( $col_vector ) . "\t" . $id . "\t" . json_encode( $col_vector );
                                    
                } else {

                    $md = Metadata::get_obj_part(   $id,
                                                    'is_dark',
                                                    array("authed"=>TRUE ) );                    
                
                    if ( isset( $md[ 'result' ] ) && ( $md[ 'result' ] == TRUE ))
                        $outline = "* DARK collection $id";

                }
                
                echo $outline . "\n";

            }
            
        }

    fclose($handle);

    }


