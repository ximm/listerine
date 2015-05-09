#!/usr/bin/env php
<?
/* require_once '/petabox/setup.inc'; */
require_once '/home/ximm/petabox/setup.inc';
require_once '/home/ximm/projects/search/Listerine.inc';


    list( $ct, $ret ) = Listerine::get_items_in_collection( 'americana', TRUE, FALSE );
    echo $ct . "\n";
    
    exit();

    $ret = Listerine::expand_subcollections( 'americana' );
    var_dump( $ret );


    echo "REDUCED:" . "\n";    
    $ret = Listerine::get_collections_for_item( 'arxiv-hep-ph9808472', FALSE );
    var_dump( $ret );    

    echo "\n";    
    
    echo "EXPANDED:" . "\n";    
    $ret = Listerine::get_collections_for_item( 'arxiv-hep-ph9808472' );
    var_dump( $ret );

    echo "\n";    

        
    $ret = Listerine::expand_subcollections( 'additional_collections' );
    var_dump( $ret );

    echo "\n";    

    // get_items_in_collection( $collection, $expand=false, $collections_only=false )    
    $ret = Listerine::get_items_in_collection( 'additional_collections', FALSE );
    var_dump( $ret );
    