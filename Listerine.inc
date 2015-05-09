<?
// Copyright(c)2005-(c)2015 Internet Archive. Software license GPL version 2.

/*

    That minty fresh new perspective on the collections graph and lists. Ahhhhhhhhhhhh...    

    Goal: answer questions about collection and list membership in a performant, truthful manner.
    
    Performant means:   amoratized cost of queries supports V2-level load, and fast responses ( < 50ms ideally)
    Truthful means:     collection and list membership accurately reflects documents as indexed, not on disk metadata;
                        documents are indexed in near-real-time (minutes latency max);
                        will NOT support 'look ahead' for pending catalog tasks
    

    Notes:  1. on-disk collection vectors are expanded, but are incomplete and graph is somewhatbroken
            2. corrected collection_expanded can be computed given asccurate collection_reduced vectors
            3. collection_reduced vectors are distillable from existing expanded vectors, but:
                3b. doing so REQUIRES multi-pass analysis of expanded vectors for collection items,
                    this is currently performed via col_graph.py or similar PRIOR TO INITIAL INDEXING
            4. once reduced vectors are properly indexed, collection_expanded vectors MUST be cached for collection and list items
            5. cached == indexed with appropriate fields in document, as documented

            Regardless of whether results are computed at runtime, or cached in indexed documents,
            
            6. truthful answers require accurate collection_expanded vectors

            7. sole source of truth for list membership is currently favorites lists in users table
                
    Relies on fields in Elasticsearch documents, in Listerine::ES_INDEX only, not archive-all variants yet:

    TODO: nested enumerated list support
        * working on now, requires paralleization of list/collection hanlding
          i.e. items must be indexed with their list_reduced just as they have collection_reduced 
    TODO: optimize by caching list graph (subset with all mediatype=collection) in one-shard DB, one copy per node, for fast lookups      
        * requires maintaining truth in two locations redundantly; we must support mixed-type results 
    TODO: list ordering (and other list memebership edge metadata)        * 
    TODO: sets API 'side channel' combining list/collection cache
    TODO: invalidation/to-re-index list generation and catalog done following
    TODO: search collection support (Lucene query passthru?)
        * we will be able to answer membership of this collection questions (give all members, expanded or not)
        * we will NOT be able to answer collection memberships of this item queries without cached expansions of all items
            ** because search queries can be based on any metadata property, every item will need to be tested for all collections on every update
                *** possibly all the time, if we allow relative time-based search queries (eg. scoped to 'a year ago through yesterday')

    NEXT: download count application!


    PREREQUISITES
    
    Currently a multi-state process is required to bootstrap from existing item metadata.
    
        collections                         CURRENTLY, collection_reduced must be computed for every collection using a graph;
                                            this is currently done by analyzing all collection metadata via col_graph.py, whose
                                            output is a dump of reduced and [sample] corrected expanded vectors for all collections
                                        
                                            IN FUTURE, when we have collection_reduced vector in metadata ground truth,
                                            one need only perform a one of these processes:

                                                TWO STEP INDEX ONLY PROCESS                                            
                                                a)  index all collections with only the available collection_reduced vector
                                                b)  reindex all collections, computing collection_expanded from indexed truth
                                                
                                                ONE STEP PROCESS
                                                a)  index using indexing code that preloads and computes collection vector
                                                    * DOES NOT YET EXIST - would marry logic in col_graph.py and ESIndexer.inc
        
        lists                               CURRENTLY, only use of lists is fav- items. Source of truth is user-table.
                    
                                            IN FUTURE, list items (collections with members.json) will exist as well.
                                            
                                            For now, list vectors may be computed using processes symmetric with the 'future' case above,
                                            because we do not use 
                                             
        
            
                    


    Preexisting fields, source of truth is item metadata:
    
        "collections"                       on-disk metadata collection vector

    Synthesized indexed fields, source of truth is users table in DB:       
    
        "listmemberships"                   those lists (fav- currently) to which item belongs
        "members"                           enumeration of members.json of a list (currently fav- )
    
    New fields in Listerine::ES_INDEX:

      collections support
              
        "collection_reduced"                collapsed (de-expanded) expandable vector
        "collection_expanded"               fully expanded version of reduced vector, determinable from collection_reduced vectors
        "sub_collection_expanded"           fully expanded set of subcollections, optionally including self

        "collection_reduced_accurately"     Boolean: whether calculated reduced vector is believed accurate (coherent and connected)

      list support

        "list_reduced"                      native set of lists to which item belongs
        "list_expanded"                     fully expanded set of lists to which item belongs, determinable from lists_reduced
        "sub_list_expanded"                 fully expanded set of sublists, to which item belong

        "members"                           enumeration of members.json of a list, SYMMETRICAL WITH LIST_REDUCED
        
      combined expansion support
        
        "set_reduced"                       union of collection_reduced and list_reduced
        "set_expanded"                      union of collection_expanded and list_expanded
        "sub_set_expanded"                   union of sub_collection_expanded and sub_list_expanded
        
      theoretical, not calculated; only for index-time expansion case
      
        "members_expanded"                  enumeration of members.json plus members of any member of any collection in members.json (and their subcollections)

*/

# runtime switches

define ('LISTERINE_RUNTIME_EXPANSION',  true );
define ('LISTERINE_NOCACHING',          true );

define ('LISTERINE_DEBUG_OUTPUT',       false );



class Listerine
{

    const   ES_INDEX =          'list-test';

    const   TYPE_COLLECTION =   'collection';
    const   TYPE_LIST =         'list';
    const   TYPE_SET =          'set';

  /*

    PUBLIC INTERFACE 

    COLLECTIONS
        
        get_collections_for_item( $id, $expanded=true )
            expanded            include all ancestors

        in_collection_P( $id, $collection  )

            * relies on caching of collection_expanded for all collections
        
        get_items_in_collection( $id, $expanded=false, $collections_only=false )
            expanded            include members of all subcollections
            collections_only    filter so only collections are returned 

        get_facets_for_collection( $bucket_term='collection_reduced'  )

            * relies on caching of sub_collection_expanded for all collections

    LISTS


        get_lists_for_item( $id, $expanded=true )
            expanded            include membership by virtue of ancestors being on a list

            * relies on caching of sub_collection_expanded for all lists

        in_list_P( $id, $list )
        
        get_items_in_list( $id, $expanded=false, $collections_only=false )
            expanded            include members of all subcollections
            collections_only    filter so only collections are returned 

            * relies on caching of sub_collection_expanded for all collections and lists
            * relies on caching of members_expanded for all lists

        get_facets_for_list( $bucket_term='collection_reduced'  )

Sample native Elasticsearch DSL query to return facets (collection membership) for all members of 'americana' 

GET list-test/_search?search_type=count
{
  "aggregations" : {
    "facets": {
      "terms": {
        "field": "collection_reduced"
      }
    }
  },
  "query" : {
    "filtered" : {
      "filter" : {
        "bool": {
          "should": [ 
            { "terms" : {
                "collection_reduced" : {
                  "index" : "list-test",
                  "type": "item",
                  "id" : "americana",
                  "path" : "sub_collection_expanded"
                },
                "_cache": "americana_expanded_items"
              }
            },
            { "term": { "collection_reduced":"americana" } }
          ]
        }
      }
    }
  }
}



  */


/*
    ------------------COLLECTION INTERFACE-----------------------------------------   
*/

  // ancestors only, does NOT include subcollections or self if collection
  public static function get_collections_for_item( $id, $expanded=true  ) 
  {
    return Listerine::get_upward_vector_for_item( $id, Listerine::TYPE_COLLECTION, $expanded ) ; 
  }

  public static function in_collection_P( $id, $collection  ) 
  {
    $item_collections = Listerine::get_upward_vector_for_item( $id, Listerine::TYPE_COLLECTION, true );
    return in_array( $collection, $item_collections );    
  }
  
  public static function get_items_in_collection( $id, $expanded=false, $collections_only=false ) 
  {
    return Listerine::get_downward_membership_for_item( $id, $list_type=Listerine::TYPE_COLLECTION, $expanded, $collections_only ); 
  }


/*
    ------------------LIST INTERFACE-----------------------------------------   
*/
  
  public static function get_lists_for_item( $id, $expanded=true  )
  {

    if ( $expanded ) {

        $query_obj = Array(     "_source"   =>  false,
                                "from"      =>  0,
                                "size"      =>  50,
                            );
    
        if ( LISTERINE_RUNTIME_EXPANSION ) {

            //  all lists whose members field includes $id, and, 
            //  all lists whose expanded list of subcollections* includes at least one member of $id's collection_reduced vector
            //    *  summed over collection members                        

            $query_obj[ "query" ] = Array(
                        "filtered" =>  Array(
                            "filter" =>  Array(
                                "bool" =>  Array(
                                    "should" => Array(  
                                        Array( "term"  => Array( "members" => $id )),
                                        Array( "terms" => Array( "collection_reduced" =>
                                                                Array(  "index" =>  Listerine::ES_INDEX,
                                                                        "type"  =>  "item",
                                                                        "id"    =>  $id,
                                                                        "path"  =>  "sub_collection_expanded" ),
//                                                                  "_cache"  =>  "expanded_lists_" . $id,
                                                                ),                                                    
                                                    ),  // terms
                                                 ),     // should
                                             ),         // bool
                                         ),             // filter
                                     ),                 // filtered
                                 );                     // query

        } else {

            //  all lists whose members_expanded field includes $id

            $query_obj[ "filter" ] = Array( "term"  => Array( "members_expanded" => $id ));

        }
        
    } else {
    
            // unexpanded (only children), all items

            $query_obj[ "filter" ] = Array( "term"  => Array( "members" => $id ));

    }

    $query = json_encode( $query_obj );
                    
    list( $result_count, $lists ) = Listerine::get_hits( $query );
        
    return $lists;
    
  }


  public static function in_list_P( $id, $list  ) 
  {

    $item_lists = Listerine::get_lists_for_item( $id, true );
    return in_array( $list, $item_lists );
    
  }

  
  public static function get_items_in_list( $id, $expanded=false, $collections_only=false ) 
  {

    $query_obj = Array(     "_source"   =>  false,
                            "from"      =>  0,
                            "size"      =>  50,
                        );
  
    if ( $expanded ) {

        if ( $collections_only ) {

            // expanded, only collections
            
            if ( LISTERINE_RUNTIME_EXPANSION ) {

                // all items whose 
                //      mediatype = collection, AND,
                //          whose identifier appears in 'members' field of list $id, OR,
                //          whose collection_reduced vector includes at least one collection in the expanded subcollections list of this list

                $query_obj[ "query" ] = Array(
                            "filtered" =>  Array(
                                "filter" =>  Array(
                                    "bool" =>  Array(
                                    
                                        "must" => Array(
                                            Array( "term"  => Array( "mediatype" => "collection" )), 
                                                        ),      // must

                                        "should" => Array(
                                            Array( "terms" => Array( "collection_reduced" =>
                                                                    Array(  "index" =>  Listerine::ES_INDEX,
                                                                            "type"  =>  "item",
                                                                            "id"    =>  $id,
                                                                            "path"  =>  "sub_collection_expanded" ),
//                                                                      "_cache"  =>  "expanded_items_" . $id,
                                                                    ),                                                    
                                                            ),  // terms
                                            Array( "terms" => Array( "id" =>
                                                                    Array(  "index" =>  Listerine::ES_INDEX,
                                                                            "type"  =>  "item",
                                                                            "id"    =>  $id,
                                                                            "path"  =>  "members" ),
                                                                    ),                                                    
                                                            ),  // terms
                                                         ),     // should

                                                 ),         // bool
                                             ),             // filter
                                         ),                 // filtered
                                     );                     // query


            } else {

                // all items whose identifier appears in 'members-expanded' field of list $id

                
                            
            }
        } else {

            // expanded, all items

            if ( LISTERINE_RUNTIME_EXPANSION ) {

                // all items whose identifier appears in 'members' field of list $id, and,
                // all items whose collection_reduced vector includes at least one collection in the expanded subcollections list of this list

                $query_obj[ "query" ] = Array(
                            "filtered" =>  Array(
                                "filter" =>  Array(
                                    "bool" =>  Array(
                                        "should" => Array(  
                                            Array( "terms" => Array( "collection_reduced" =>
                                                                    Array(  "index" =>  Listerine::ES_INDEX,
                                                                            "type"  =>  "item",
                                                                            "id"    =>  $id,
                                                                            "path"  =>  "sub_collection_expanded" ),
//                                                                      "_cache"  =>  "expanded_items_" . $id,
                                                                    ),                                                    
                                                        ),  // terms
                                            Array( "terms" => Array( "id" =>
                                                                    Array(  "index" =>  Listerine::ES_INDEX,
                                                                            "type"  =>  "item",
                                                                            "id"    =>  $id,
                                                                            "path"  =>  "members" ),
                                                                    ),                                                    
                                                        ),  // terms
                                                     ),     // should
                                                 ),         // bool
                                             ),             // filter
                                         ),                 // filtered
                                     );                     // query


                $query = json_encode( $query_obj );
                
                list( $result_count, $hits ) = Listerine::get_hits( $query );


            } else {

                $lists = Listerine::get_raw_members_vector( $id, true );
                $result_count = count( $lists );
                                            
            }

        }
    
    } else {

        // unexpanded (only children), only collections
    
        if ( $collections_only ) {

            // all items
            //   whose mediatype = collection, AND
            //   whose identifier appears in 'members' field of list $id

            $query_obj[ "query" ] = Array(
                            "filtered" =>  Array(
                                "filter" =>  Array(
                                    "bool" =>  Array(
                                        "must"  =>  Array(  Array( "term"  => Array( "mediatype" => "collection" )),
                                                            Array( "terms" => Array( "id" =>
                                                                Array(  "index" =>  Listerine::ES_INDEX,
                                                                        "type"  =>  "item",
                                                                        "id"    =>  $id,
                                                                        "path"  =>  "members" ),
                                                                ),
                                                            ),  // terms 
                                                     ),     // must
                                                 ),         // bool
                                             ),             // filter
                                         ),                 // filtered
                                     );                     // query

            $query = json_encode( $query_obj );
                            
            list( $result_count, $hits ) = Listerine::get_hits( $query );

        } else {
    
            // unexpanded (only children), all items

            $lists = Listerine::get_raw_members_vector( $id );
            $result_count = count( $lists );

        }

    }    
            
    return Array( $result_count, $hits );
    
  }


    // for now, lists are limited to user favorites, the source of truth of which is the users table
    public static function get_favorites_lists_for_item( $id )
    {
        $list_names = DB::qvals("SELECT itemname FROM users WHERE id IN (SELECT user_id FROM bookmarks WHERE ",array('identifier'=>$id),") AND locked!=1");
    
        if ( count( $list_names ) ) { 
            foreach ( $list_names as $list_name ){
              $a_list = 'fav-'.ltrim( $list_name, '@' ); // convert to favorites collection name
              $lists[] = $a_list;
            }
        }
    }

    
 
  /*
  
    PRIVATE  
  
  */

  // ancestors only, does NOT include subcollections or self if collection type
  private static function get_upward_vector_for_item( $id, $list_type=Listerine::TYPE_COLLECTION, $expanded=true  ) 
  {

    if ( $expanded )
    
        if ( LISTERINE_RUNTIME_EXPANSION )

            $vector = Listerine::expand_ancestors( $id, $list_type );

        else

            $vector = Listerine::get_raw_upward_vector_for_item( $id, $list_type, true );
        
    else
    
        // unexpanded
        
        $vector = Listerine::get_raw_upward_vector_for_item( $id, $list_type );         // default is reduced
    
    return $vector;
    
  }

  // TODO: implement scanning and/or paging
  // TODO: support sorting
  public static function get_downward_membership_for_item( $id, $list_type=Listerine::TYPE_COLLECTION, $expanded=false, $collections_only=false ) 
  {
    
    $query_obj = Array(     "_source"   =>  false,
                            "from"      =>  0,
                            "size"      =>  50,
                        );

    if ( $expanded ) {

        if ( $collections_only ) {

            // expanded, only collections
            
            $hits = Listerine::expand_subcollections( $id );

        } else {

            // expanded, all items

            if ( LISTERINE_RUNTIME_EXPANSION ) {

                $vector_field = Listerine::get_field_name( $list_type, false );

                $subcollection_vector_field = Listerine::get_field_name( $list_type, true, true );

                $query_obj[ "query" ] = Array(
                            "filtered" =>  Array(
                                "filter" =>  Array(
                                    "bool" =>  Array(
                                        "should" => Array(  
                                            Array( "terms" => Array( $vector_field =>
                                                                    Array(  "index" =>  Listerine::ES_INDEX,
                                                                            "type"  =>  "item",
                                                                            "id"    =>  $id,
                                                                            "path"  =>  $subcollection_vector_field ),
//                                                                      "_cache"  =>  "expanded_items_" . $list_type . "_" . $id,
                                                                    ),                                                    
                                                        ),  // terms
                                            Array( "term"  => Array( $vector_field => $id )),
                                                     ),     // should
                                                 ),         // bool
                                             ),             // filter
                                         ),                 // filtered
                                     );                     // query


            } else {

                $vector_field = Listerine::get_field_name( $list_type, true );

                $query_obj[ "filter" ] =    Array(  "term"  =>  Array(   $vector_field  =>  $id ) );
                            
            }

                $query = json_encode( $query_obj );
                
                list( $result_count, $hits ) = Listerine::get_hits( $query );

        }
    
    } else {

        $vector_field = Listerine::get_field_name( $list_type, false );
    
        if ( $collections_only ) {

            // unexpanded (only children), only collections

            $query_obj[ "query" ] = Array(
                            "filtered" =>  Array(
                                "filter" =>  Array(
                                    "bool" =>  Array(
                                        "must"  =>  Array(  Array( "term"  => Array( $vector_field => $id )),
                                                            Array( "term"  => Array( "mediatype" => "collection" ))
                                                     ),     // must
                                                 ),         // bool
                                             ),             // filter
                                         ),                 // filtered
                                     );                     // query

        } else {
    
            // unexpanded (only children), all items
            
            $query_obj[ "filter" ] =    Array(  "term"  =>  Array(   $vector_field   =>  $id ) );                        

        }

        $query = json_encode( $query_obj );
                            
        list( $result_count, $hits ) = Listerine::get_hits( $query );

    }    
            
    return Array( $result_count, $hits );
    
  }

  
  private static function get_hits( $query, $field_of_interest='_id' )
  {

    if ( LISTERINE_DEBUG_OUTPUT )
        echo $query . "\n";

    list( $success, $reply_obj ) = Listerine::es_dsl( $query );

    if ( LISTERINE_DEBUG_OUTPUT )
        var_dump( $reply_obj );

    $hits = Array();
        
    if ( $success && (isset( $reply_obj[ 'hits' ] ))) {
        $result_count = $reply_obj[ 'hits' ][ 'total' ];
        foreach( $reply_obj[ 'hits' ][ 'hits' ] as $hit ) {
            // extract field we're after, which may be top level (_ fields) or inside _source
            if ( $field_of_interest[0] === '_' )
                $target_field = $hit[ $field_of_interest ];
            else
                $target_field = $hit[ '_source' ][ $field_of_interest ];
            if (isset( $target_field ))
                if ( is_array( $target_field ) )
                    $hits = array_merge( $hits, $target_field );
                else
                    $hits[] = $target_field;
        }
    }

    if ( LISTERINE_DEBUG_OUTPUT ) {
        echo "Matched: " . $result_count . "\n";
        var_dump( $hits );
    }
    
    return Array( $result_count, $hits );
    

  }

  // get raw vector out of ES document for $id 
  private static function get_raw_upward_vector_for_item( $id, $list_type=Listerine::TYPE_COLLECTION, $expanded=false )
  {
  
        $vector_field = Listerine::get_field_name( $list_type, $expanded );
                
        $query_obj = Array(     "_source"   =>  Array(  $vector_field ),
                                "filter"    =>  Array(  "term"   =>
                                                            Array(   "_id"  =>  $id ),
                                                     ),
                            );
    
        $query = json_encode( $query_obj );
                                    
        list( $result_count, $collections ) = Listerine::get_hits( $query, $vector_field );

        if ( LISTERINE_DEBUG_OUTPUT )
            var_dump( $collections );

        return $collections;            

  }

  // get members field out of ES document 
  private static function get_raw_members_vector( $id, $expanded=false )
  {
        if ( $expanded )
            $members_field = "members_expanded";
        else
            $members_field = "members";

        
        $query_obj = Array(     "_source"   =>  Array(  $members_field ),
                                "filter"    =>  Array(  "term"   =>
                                                            Array(   "_id"  =>  $id ),
                                                     ),
                            );
    
        $query = json_encode( $query_obj );
                                    
        list( $result_count, $lists ) = Listerine::get_hits( $query, $members_field );

        if ( LISTERINE_DEBUG_OUTPUT )
            var_dump( $lists );

        return $lists;            

  }


  // scan = duration to keep context of scan to ensure novel/complete results each time e.g. '1m'
  private static function es_dsl( $query, $scan=false )
  {
  
    $query_base_url = 'http://es-lb:9200/' . Listerine::ES_INDEX . '/_search';
    if ( $scan )
        $query_base_url .= '?scan=' . $scan . '&search_type=scan';
        
    $cmd = 'curl --silent --location -XGET ' . '"' . $query_base_url . '"' .  " -d" . "'" . $query . "'" ;

    $response = Util::cmd( $cmd, 'STRING' );

    if ( LISTERINE_DEBUG_OUTPUT )
        echo $response . "\n";

/*        
      // TODO: abandoned curl wrapper as escape-encoding issue seemed intractable
      
      $query_url = $query_base_url;
      
      echo $query_url . "\n";
      echo $query . "\n";
      
      $ci = curl_init();
      curl_setopt($ci, CURLOPT_URL, $query_url );
      curl_setopt($ci, CURLOPT_PORT, 9200);
      curl_setopt($ci, CURLOPT_TIMEOUT, 2000);
      curl_setopt($ci, CURLOPT_RETURNTRANSFER, 1);
      curl_setopt($ci, CURLOPT_FORBID_REUSE, 0);
      curl_setopt($ci, CURLOPT_CUSTOMREQUEST, 'POST');
      curl_setopt($ci, CURLOPT_POSTFIELDS, $query );
      
      $response = curl_exec($ci);
*/
      
    $reply_obj = json_decode($response, 1);
      
    $success = (isset( $reply_obj[ 'error' ]) == false );
  
    $reply = Array( $success, $reply_obj );
      
    return $reply;
            
  }
  
  // DEPRECATED
  private static function query_for_expanded_subcollections( $id )
  {

    $subs = Listerine::expand_subcollections( $id );

    if (!count( $subs ))
        return false;
        
    $terms_arr = Array();
    foreach( $subs as $sub ) {
        $terms_arr[] = Array( "term"  =>  Array( "collection" =>  $sub ) );
    }
    
    $query_obj = Array(
                                "query"     => Array(
                                "filtered" =>  Array(
                                    "filter" =>  Array(
                                        "bool" =>  Array(
                                            "minimum_should_match" =>   1,
                                            "should"  =>  $terms_arr
                                                     ),         // bool
                                                 ),             // filter
                                             ),                 // filtered
                                         ),                     // query
                        );        

    return $query_obj;
  
  }



  /*

    GRAPH SERVICE
  
    Answer collections graph questions in a performant manner, using a combination of ES and Redis.

    For any collection, answer with reasonably timely (but imperfect) reflection of on-disk truth: 
        - expanded parents              ie      recursive parents           ie      reduced -> expanded
        - expanded subcollections       ie      recursive descendants       ie      fully expanded children

    Upward expansion to expanded vectors can be done using recursive logic and from reduced vectors.
        Caching result will yield fast replies.
        Indexing result will provide for downward expansion.
        
    Downward expansion requires query against expanded vectors in Elasticsearch.
        Search against expanded vectors will yield fast replies and can support large response sets.
    
        
  */

  // this operation assumes that we have accurate up-to-date _expanded vectors in search scope
  public static function expand_subcollections( $id, $list_type=Listerine::TYPE_COLLECTION ) 
  {
  
    $field_name = Listerine::get_field_name( $list_type, true );

    $query_obj = Array(     "_source"   =>  false,
                            "from"      =>  0,
                            "size"      =>  10000,              // should be sufficient to get them *all*... gulp
                            "query"     => Array(
                                "filtered" =>  Array(
                                    "filter" =>  Array(
                                        "bool" =>  Array(
                                            "must"  =>  Array(  Array( "term"  => Array( $field_name => $id )),
//                                                                Array( "term"  => Array( "mediatype" => "collection" ))
                                                         ),     // must
                                                     ),         // bool
                                                 ),             // filter
                                             ),                 // filtered
                                         ),                     // query
                        );
            
    $query = json_encode( $query_obj );
                            
    list( $result_count, $hits ) = Listerine::get_hits( $query );

    return $hits;
    
  }


  // recursively expand the graph nodes 
  // caches results if LISTERINE_NOCACHING is false  
  private static function expand_ancestors( $id, $list_type=Listerine::TYPE_COLLECTION, &$ancestor_list=Array() )
  {
    
    $parents = (array) Listerine::retrieve_value( $id, $list_type );
                    
    foreach ($parents as $parent) {
    
        if (in_array( $parent, $ancestor_list, true))
            continue;

        $ancestor_list[] = $parent;
        
        $parent_ancestor_list = Listerine::expand_ancestors( $parent, $list_type, $ancestor_list );

        foreach ( $parent_ancestor_list as $candidate )
            if (!in_array( $candidate, $ancestor_list, true))
                $ancestor_list[] = $candidate;
    }
    
    return $ancestor_list;
    
  }


  

  /*
  
    REDIS CACHE via Cache.inc

  */
  
  // note: passively caches

  private static function retrieve_value( $name, $list_type=Listerine::TYPE_COLLECTION, $secsAlive=600  )
  {
    
    if ( LISTERINE_NOCACHING ) {

        
        $col = Listerine::get_collections_for_item( $name, false );     // get reduced vector straight from ES
        return $col;
    
    } else {

        $ret = Cache::value(    "LISTERINE-GRAPHNODE-" . $name,
                                $secsAlive,
                                function() use ( &$name, &$list_type)
                                {
                                    if ( $list_type == Listerine::TYPE_COLLECTION )
                                        $col = Listerine::get_collections_for_item( $name, false );
                                    else
                                        $col = Listerine::get_lists_for_item( $name, false );
                                    $j = json_encode( $col );
                                  return $j;
                                } );
    
        return json_decode( $ret, 1 );

    }
  
  }  
  
  
  // proactive caching

  // populate the graph node data in Redis, currently, for a brief period
  public static function cache_graph_data( $expiration=600 )
  {
    // get current count 
    
    $query_obj = Array(     "_source"   =>  false,
                            "size"      =>  0,
                            "filter"    =>  Array(      
                                                      "term"   =>
                                                        Array(   "mediatype"  =>  "collection" ),
                                                 ),
                        );
    
    $query = json_encode( $query_obj );
  
    list( $success, $reply_obj ) = Listerine::es_dsl( $query );

    $collections_to_cache = 0;
    if ( $success && (isset( $reply_obj[ 'hits' ] )) && (isset( $reply_obj[ 'hits' ][ 'total' ] )))
        $collections_to_cache = intval( $reply_obj[ 'hits' ][ 'total' ] );

    if ( LISTERINE_DEBUG_OUTPUT )
        echo "Collections to cache: " . $collections_to_cache .  "\n";

    $query_obj = Array(     "_source"   =>  Array(      "collection_reduced" ),
                            "size"      =>  100,
                            "filter"    =>  Array(        
                                                        "term"   =>
                                                        Array(   "mediatype"  =>  "collection" ),
                                                 ),
                        );

    while ( $collections_to_cache > 0 )
    {
        list( $success, $reply_obj ) = Listerine::es_dsl( $query, '1m' );

        if ( $success && (isset( $reply_obj[ 'hits' ] ))) {

            $collections_cached = 0;
            foreach( (array)$reply_obj[ 'hits' ] as $hit ) {                
                Listerine::cache_value( $hit[ '_id' ],
                                        $hit[ '_source' ][ 'collection_reduced' ],
                                        $expiration );
                $collections_cached += 1;                                        
            }

            if ( LISTERINE_DEBUG_OUTPUT )
                echo "Cached this scan: " . $collections_cached .  "\n";
            
            $collections_to_cache -= $collections_cached;
        }
    }
  
  }
  
  private static function cache_value( $name, $val, $secsAlive )
  {

    $ret = Cache::value(    "LISTERINE-GRAPHNODE-" . $name,
                            $secsAlive,
                            function() use (&$val)
                            {
                              return $val;
                            } );

    return $ret;
  
  }  
  
  private static function get_field_name( $list_type, $expanded, $subcollections=false )
  {
                    
        if ( $expanded )
            $vector_field = $list_type . "_expanded";
        else
            $vector_field = $list_type . "_reduced";

        if ($subcollections)
            $vector_field .= "sub_" . $vector_field;
            
        return $vector_field;
        
  }
  
  
  private static function get_membership_matches($list) {
    $ci = curl_init();
    curl_setopt($ci, CURLOPT_URL,
                'http://es-lb:9200/' . Listerine::ES_INDEX . '/item/_search?q=listmemberships:'
                . $list . '&size=10000');
    curl_setopt($ci, CURLOPT_PORT, 9200);
    curl_setopt($ci, CURLOPT_TIMEOUT, 200);
    curl_setopt($ci, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ci, CURLOPT_FORBID_REUSE, 0);
    curl_setopt($ci, CURLOPT_CUSTOMREQUEST, 'GET');
    /* curl_setopt($ci, CURLOPT_CUSTOMREQUEST, 'POST'); */
/*     $post = ' */
/* { */
/*   "query" : { */
/*         "term" : { "listmemberships": "' . $list . '"} */
/*     } */
/* }'; */
/*     curl_setopt($ci, CURLOPT_POSTFIELDS, $post); */

    $response = curl_exec($ci);

    echo $response . "\n";

    $matches_doc = json_decode($response, 1);
    $match_ids = array();
    foreach($matches_doc['hits']['hits'] as $hit) {
      $match_ids[] = $hit['_id'];
    }
    return $match_ids;
  }

}


