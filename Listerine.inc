<?
// Copyright(c)2005-(c)2015 Internet Archive. Software license GPL version 2.

/*

    That minty fresh new perspective on the collections graph and lists. Ahhhhhhhhhhhh...    

    Goal: answer questions about collection and list membership in a performant, truthful, maintainable manner.
    
    Performant means:       amoratized cost of queries supports V2-level load, and fast responses ( < 50ms ideally)

    Truthful means:         collection and list membership accurately reflects documents as indexed, not on disk metadata;
                            documents are indexed in near-real-time (minutes latency max);
                            will NOT support 'look ahead' for pending catalog tasks

    Maintainable means:     indexing overhead is mostly limited to updates to the COLLECTIONS GRAPHS.
                            changes in list membership or collection parentage invalidate only records in parent and children collections
                            changes in the collection graph only necessitate reindexing of items when items are added/removed from lists
                            otherwise, items appear in results automatically as a result of their _reduced vectors matching updated sub_<>_expanded vectors                    
    

    Notes:  
    
            COLLECTIONS
            
            1. status quo on-disk 'collection' vectors are expanded, but are sometimes incomplete, and graph is somewhat broken
            2. normalized collection_expanded vectors can be computed, given accurate graph comprising collection_reduced vectors
            3. collection_reduced vectors are distillable from existing on disk vectors, but:
                3b. doing so REQUIRES multi-pass analysis of expanded vectors for collection items,
                    this is currently performed via col_graph.py
                    results BOOTSTRAP the collection graph, which much happen PRIOR TO ITEM INDEXING
                    calculation of item reduced vectors is also currently done in col_graph.py, using the collection graph
            4. once reduced vectors are properly indexed, collection_expanded vectors MUST be cached for collection and list items
            5. cached == indexed with appropriate fields in document, as documented

            Regardless of whether results are computed at runtime, or cached in indexed documents,
            
            6. truthful answers require accurate collection_expanded vectors

            7. sole source of truth for list membership is currently favorites lists in users table

            LISTS
            
            1. status quo on-disk favorites lists are stored in users table, cached in members.json in items
            2. to support queries, we invert membership to list_reduced (parent list==listmembership) and cache resulting graph in parallel with collection graph
            3. list membership is hence cached in two places: 
                3a. implied in parentage (list_reduced)
                3b. ordering is preserved in the enumeration in 'members_original' in list collection items
            4. list_expanded and sub_list_expanded cache list graph to support queries for access to inherited membership 
        
            SETS
        
            1. sets are the recursive deep union of collection and list membership
            2. this union is [much] larger than the simple union of the pre-computed _expanded collection and list vectors 
            3. this union represents the combined inherited inclusion of items in lists and collections through either path
                3a. ie. reveals items I1-N in collection C2 which is in collection C1 which is in favorites list L1
                3b. ie. reveals items I1-N in collection C3 which is in favorites list L2 which is in favorites list L1
                etc.
            4. this union may require trimming heuristics in the future, as favoriting top-level collections requires
            
                
    Relies on fields in Elasticsearch documents, in Listerine::ES_INDEX only, not archive-all variants yet:

    TODO: invalidation/to-re-index list generation and catalog done following

    TODO: optimize by caching list graph (subset with all mediatype=collection) in one-shard DB, one copy per node, for fast lookups      
        * requires maintaining truth in two locations redundantly; we must support mixed-type results 
    TODO: expose list ordering preserved in members_original
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
                                        
                                            IN THE FUTURE, when we have collection_reduced vector in metadata ground truth,
                                            one need only perform a one of these processes:

                                                TWO STEP INDEX ONLY PROCESS                                            
                                                a)  index all collections with only the available collection_reduced vector
                                                b)  reindex all collections, computing collection_expanded from indexed truth
                                                
                                                ONE STEP PROCESS
                                                a)  index using indexing code that preloads and computes collection vector
                                                    * DOES NOT YET EXIST - would marry logic in col_graph.py and ESIndexer.inc
        
        lists                               CURRENTLY, only use of lists is fav- items. Source of truth is user-table.
                    
                                            IN THE FUTURE, list items (collections with members.json) will exist as well.
                                            
                                            For now, list vectors may be computed using processes symmetric with the 'future' case above,
                                            because we do not use 


    Preexisting fields, source of truth is item metadata:
    
        "collection"                        on-disk metadata collection vector

    Preexisting synthesized fields, source of truth is users table in DB:
    
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

        "list_expanded_accurately"          Boolean: whether calculated expanded vector is believed accurate (coherent and connected)

        "members_original"                  enumeration of members.json of a list, SYMMETRICAL WITH LIST_REDUCED; UNANALYZED version
        
      combined expansion support
        
        "set_reduced"                       union of collection_reduced and list_reduced
        "set_expanded"                      union of collection_expanded and list_expanded
        "sub_set_expanded"                   union of sub_collection_expanded and sub_list_expanded
        
*/

# runtime switches

define ('LISTERINE_NOCACHING',          true );

define ('LISTERINE_DEBUG_OUTPUT',       false );



class Listerine
{

    const   ES_INDEX =          'listerine';

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


    SETS


        get_sets_for_item( $id, $expanded=true )
            expanded            include membership by virtue of ancestors being on a set

            * relies on caching of sub_collection_expanded for all sets

        in_set_P( $id, $set )
        
        get_items_in_set( $id, $expanded=false, $collections_only=false )
            expanded            include members of all subcollections
            collections_only    filter so only collections are returned 

            * relies on caching of sub_collection_expanded for all collections and sets
            * relies on caching of members_expanded for all sets

        get_facets_for_set( $bucket_term='collection_reduced'  )

    GRAPH OPERATIONS

        expand_subcollections( $id, $list_type=Listerine::TYPE_COLLECTION )
            * assumes _expanded vectors have been INDEXED, works by ES query        
        expand_ancestors( $id, $list_type=Listerine::TYPE_COLLECTION, &$ancestor_list=Array() )
            * recursive function computing _expanded vectors, ASSUMES _reduced have been bootstrapped into a coherent graph
    
        invalidation_list_for_collection( $id, $calculate_delta=false )         
        invalidation_list_for_item( $id, $calculate_delta=false )

            * invalidation functions provide lists of identifiers to re-index, when collection/list preoperties of $id change
            
  */


/*
    ------------------COLLECTION INTERFACE-----------------------------------------   
*/

  // ancestors only, does NOT include subcollections or self if collection
    public static function get_collections_for_item( $id, $expanded=true  ) 
    {
        return Listerine::get_upward_vector_for_item( $id, Listerine::TYPE_COLLECTION, $expanded ) ; 
    }

    public static function get_original_collections_for_item( $id ) 
    {

        $rep = Metadata::get_obj_part(   $id,
                                        'metadata/collection',
                                        array("authed"=>TRUE,
                                        'primaryonly'=>FALSE) );
                                                
        try {

            $original_collection = (Array)$rep[ "result" ];
        
        } catch (Exception $e) {

            $original_collection = false;
        
        }

        return $original_collection;
    }


    public static function in_collection_P( $id, $collection  ) 
    {
        $item_collections = Listerine::get_upward_vector_for_item( $id, Listerine::TYPE_COLLECTION, true );
        return in_array( $collection, $item_collections );    
    }
  
    public static function get_items_in_collection( $id, $expanded=false, $collections_only=false ) 
    {
        return Listerine::get_downward_membership_for_item( $id, Listerine::TYPE_COLLECTION, $expanded, $collections_only ); 
    }

    public static function get_facets_for_collection( $id, $expanded=false, $bucket_term='collection_reduced' )
    {
        return Listerine::get_facets( $id, $expanded, $list_type=Listerine::TYPE_COLLECTION, $bucket_term ); 
    }


/*
    ------------------LIST INTERFACE-----------------------------------------   
*/

    // ancestors only, does NOT include subcollections or self if collection
    public static function get_lists_for_item( $id, $expanded=true  ) 
    {
        return Listerine::get_upward_vector_for_item( $id, Listerine::TYPE_LIST, $expanded ) ; 
    }

    public static function in_list_P( $id, $collection  ) 
    {
    $item_collections = Listerine::get_upward_vector_for_item( $id, Listerine::TYPE_LIST, true );
        return in_array( $collection, $item_collections );    
    }
  
    public static function get_items_in_list( $id, $expanded=false, $collections_only=false ) 
    {
        return Listerine::get_downward_membership_for_item( $id, Listerine::TYPE_LIST, $expanded, $collections_only ); 
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

   public static function get_facets_for_list( $id, $expanded=false, $bucket_term='list_reduced' )
    {
        return Listerine::get_facets( $id, $expanded, $list_type=Listerine::TYPE_LIST, $bucket_term ); 
    }

/*
    ------------------SET INTERFACE-----------------------------------------   
*/

  // ancestors only, does NOT include subcollections or self if collection
    public static function get_sets_for_item( $id, $expanded=true  ) 
    {
        return Listerine::get_upward_vector_for_item( $id, Listerine::TYPE_SET, $expanded ) ; 
    }

    public static function in_set_P( $id, $collection  ) 
    {
        $item_collections = Listerine::get_upward_vector_for_item( $id, Listerine::TYPE_SET, true );
        return in_array( $collection, $item_collections );    
    }
  
    public static function get_items_in_set( $id, $expanded=false, $collections_only=false ) 
    {
        return Listerine::get_downward_membership_for_item( $id, $Listerine::TYPE_SET, $expanded, $collections_only ); 
    }

  public static function get_facets_for_set( $id, $expanded=false, $bucket_term='set_reduced' )
    {
        return Listerine::get_facets( $id, $expanded, $list_type=Listerine::TYPE_SET, $bucket_term ); 
    }    
 
 
 
    /*

    PRIVATE  

    */

    // ancestors only, does NOT include subcollections or self if collection type
    private static function get_upward_vector_for_item( $id, $list_type=Listerine::TYPE_COLLECTION, $expanded=true  ) 
    {

        if ( $expanded )
    
            $vector = Listerine::expand_ancestors( $id, $list_type );
        
        else
    
            // unexpanded
        
            $vector = Listerine::get_raw_upward_vector_for_item( $id, $list_type );         // default is reduced
    
        return $vector;
    
    }

    // TODO: implement scanning and/or paging
    // TODO: support sorting
    private static function get_downward_membership_for_item( $id, $list_type=Listerine::TYPE_COLLECTION, $expanded=false, $collections_only=false ) 
    {
    
        $query_obj = Array(     "_source"   =>  false,
                                "from"      =>  0,
                                "size"      =>  2000,
                            );

        if ( $expanded ) {

            //
            // EXPANDED
            //

            if ( $collections_only ) {

                // expanded, only collections
            
                $hits = Listerine::expand_subcollections( $id );

            } else {

                // expanded, all items

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


                $query = json_encode( $query_obj );
            
                list( $result_count, $hits ) = Listerine::get_hits( $query );

            }
    
        } else {

            //
            // UNEXPANDED        
            //
        
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


    private static function get_facets( $id, $list_type=Listerine::TYPE_COLLECTION, $bucket_term='collection_reduced' )
    {

        $vector_field = Listerine::get_field_name( $list_type, $expanded );
        $sub_vector_field = Listerine::get_field_name( $list_type, true, true );

        $query_obj = Array(     "_source"   =>  false,
                                "from"      =>  0,
                                "size"      =>  2000,
                            );


        $query_obj[ "aggregations" ] = Array(
                        "facets" =>  Array(
                            "terms" =>  Array(
                                "field" =>  $bucket_term    )));

        if ( $expanded ) {

            //
            // EXPANDED
            //

            // aggregate against all items with $id as parent, or, whose parent appears in list of sub_collections_expanded of $id
            
            $query_obj[ "query" ] = Array(
                            "filtered" =>  Array(
                                "filter" =>  Array(
                                    "bool" =>  Array(
                                        "should"  =>  Array(
                                            Array( "term"   => Array( $vector_field => $id )),
                                            Array( "terms"  => Array( $vector_field =>
                                            
                                                //  MAGIC: pull 'terms' query match list from field of item in index foo
                                                //  NOTE: terms matches are unanalyzed
                                            
                                                Array(
                                                    "index" =>  Listerine::ES_INDEX,
                                                    "type"  =>  "item",
                                                    "id"    =>  $id,
                                                    "path"  =>  $sub_vector_field       //  "sub_collection_expanded"
                                                ))
                                                        ),  // terms
                                                     ),     // should
                                                 ),         // bool
                                             ),             // filter
                                         ),                 // filtered
                                     );

        } else {
        
            //
            // UNEXPANDED
            //

            // aggregate against all items with $id as parent only

            $query_obj[ "query" ] = Array(
                            "filtered" =>  Array(
                                "filter" =>  Array( "term"   => Array( $vector_field => $id )
                                             ),             // filter
                                         ),                 // filtered
                                     );

        }

        $query = json_encode( $query_obj );

        list( $success, $reply_obj ) = es_dsl( $query, 'count' );           //  search_type=count means no results needed, just perform aggregation

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
            $members_field = "members_original";

        
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
    private static function es_dsl( $query, $search_type=false )
    {
  
        $query_base_url = 'http://es-lb:9200/' . Listerine::ES_INDEX . '/_search';

        if ( $search_type ) {
            $query_base_url .= '?search_type=' . $search_type;
            if ( $search_type == 'scan' )
                $query_base_url .= '&scan=1m' . '&search_type=scan';
        }
        
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


    /*

        GRAPH SERVICE

        Answer collections graph questions leveraging ES terms query, and maybe Redis

        For any collection, list, or set, answer with reasonably timely (but imperfect) reflection of on-disk truth: 
            - expanded parents              ie      recursive parents           ie      reduced -> expanded
            - expanded subcollections       ie      recursive descendants       ie      fully expanded children collections


        Upward expansion to expanded vectors can also be done using recursive logic and from reduced vectors
            Indexing _expanded fields provides for subsequent second pass to cache downward expansions.
    
        Downward expansion REQUIRES query against sub_<foo>_expanded vectors in Elasticsearch
            Queries against expanded vectors yield fast replies and support large response sets.
            
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

    // public callers shoud USUALLY use get_<foo>_for_item(), which uses existing cached version
    //  this version provides run-time expansion but recurses, costing N queries

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
  
    // returns ( reduced vector, expanded vector ) 
    public static function reduce_collection_vector( $id, $original_collections_vector=false )
    {
        
        if ( !$original_collections_vector )
            $original_collections_vector = Listerine::get_original_collections_for_item( $id );

        if ( LISTERINE_DEBUG_OUTPUT )
            var_dump( $original_collections_vector );

        $accurately_reduced_parents = false;
        $reason = '';

        $vector_expanded = Array();

        foreach( $original_collections_vector as $parent ) {
        
            if (! in_array( $parent, $vector_expanded ) )
                $vector_expanded[] = $parent;
        
            $parent_ancestors = Listerine::get_collections_for_item( $parent, true );

            if ($parent_ancestors)            
                foreach( $parent_ancestors as $ancestor )                
                    if (! in_array( $ancestor, $vector_expanded ) )
                        $vector_expanded[] = $ancestor;
            
        }

        $vector_reduced = Array();

        foreach( $vector_expanded as $ancestor_in_expansion )        
            if ( in_array( $ancestor_in_expansion, $original_collections_vector ) )
                $original_collections_vector = array_diff( $original_collections_vector, Array( $ancestor_in_expansion ) );     // remove ancestor from original vector

        if ( count( $vector_expanded ) > 0 )
            $vector_reduced[] = $vector_expanded[ 0 ];

        // reduced = proper parent, and, anything in the original vector not in the summed 
        $vector_reduced = array_merge( $vector_reduced, $original_collections_vector );

        return Array( $vector_reduced, $vector_expanded );
  
    }
    
    /*
        INVALIDATION
    
    */
    
    // the list of all items which need to be reindexed, to 
    // TODO: optimization potential: process calculated fields now, if we can, as hints

    // collections
    //      everything in sub_set_exetended
    //      everything in set_exetended 
    //      everything in members_original, which is itself 

    public static function invalidation_list_for_collection( $id, $calculate_delta=false )
    {
    
        $query_obj = Array(     "_source"   =>  false,
                                "from"      =>  0,
                                "size"      =>  10000,              // should be sufficient to get them *all*... gulp
                                "query"     => Array(
                                    "filtered" =>  Array(
                                        "filter" =>  Array(
                                            "bool" =>  Array(
                                                "should"  =>  Array(

                                                    // everything that has $id in its list of subcollections/lists *might* be affected                                                        
                                                    Array( "term"  => Array( "sub_set_expanded" => $id )),

                                                    // everything that has $id in its list of parent collections/lists *might* be affected
                                                    Array( "term"  => Array( "set_expanded" => $id )),

                                                    // everything that has $id as a proper list parent (in its list_reduced)
                                                    Array( "term"  => Array( "list_reduced" => $id ))

                                                             ),     // should
                                                         ),         // bool
                                                     ),             // filter
                                                 ),                 // filtered
                                             ),                     // query
                        );
                    
        $query = json_encode( $query_obj );
                            
        list( $result_count, $hits ) = Listerine::get_hits( $query );

        return $hits;
    
    }
    

    //  items
    //      every list whose members_original lists $id

    public static function invalidation_list_for_item( $id, $calculate_delta=false )
    {    

        $query_obj = Array(     "_source"   =>  false,
                                "from"      =>  0,
                                "size"      =>  10000,              // should be sufficient to get them *all*... gulp
                                "filter"    => Array(
                                    "term" =>  Array(  "members_original"   => $id )
                                    )
                            );
            
        $query = json_encode( $query_obj );
                            
        list( $result_count, $hits ) = Listerine::get_hits( $query );

        return $hits;
    
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
            list( $success, $reply_obj ) = Listerine::es_dsl( $query );  // TODO: decorate with 1m cache

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

}


