# listerine

Real-time truth about Archive lists and collections graph, leveraging Elasticseach.

Principle: cache collection graph to avoid necessity of fully denormalized collection/list ancestry in every item.
Motivation: reduce re-indexing on invalidation from simple UI actions two orders of magnitude
Powered by: ES DSL terms filter ability to perform indirection


   RECIPE

   NOTE:     indexing cleaned items relies on graph being fully indexed with sub-collection/list/set, items cannot be correctly indexed UNTIL INDEX IS POPULATED WITH GRAPH

   1. Generate raw dump of all items with their collection and member vectors, e.g. from Elasticdump from an existing index, Metamgr, iamine, and/or users table (for membership)
   2. Process COLLECTIONS:
           derive collection_reduced and then collection_expanded vector (from original collection vector);
           derive list_reduced then list_expanded by inverting members (from members.json); and
           synthesize set_reduced and set_expanded as a recursive deep union of above (NOT a simple union of individual _expanded vectors!)
      This script dumps results, one json dict per item per line, for subsequent indexer ingest (see es_indexer.php)
   3. INDEX COLLECTIONS into a clean ES index. Only reduced and expanded set and collection matter; sub-collection/list/set fields CANNOT be calculated on first pass
           * disable sub-collection queries in indexer for fast indexing (in ESIndexer.php).
   4. REINDEX COLLECTIONS. This time, indexing can populate sub-collection/list/set fields (since _expanded values are now completely searchable in the index)
   5. Process ITEMS, dumping one json dict per item per line, only relevant field is rcollection_reduced
   6. INDEX ITEMS with collection_reduced from dump and calculate list_reduced using queries (into same ES index holding collection graph data)
           * membership retrieved via Listerine which makes ES query in es_indexer
   7. Listerine API can now answer questions about collection, list, and set membership in both immediate (reduced) and expanded cases, and produce facets

   TERMINOLOGY

   collection      item with mediatype=collection,                 graph truth computed from by parent vector
   list            item with mediatype=collection, members.json,   graph truth inverted from members lists (currently: users table)
   set             combination of above mediatype=collection       inclusive recursive deep union, accumulating all parents of BOTH types at EVERY ancestor
       
   FOR ALL COLLECTIONS, 
    ensure we have information to

   A. collection graph aspect              determined by ancestor relationships
       1.  compute collection_reduced          using existing metadata, using graph reduction
       2.  compute collection_expanded         using collection_reduced vectors
       3.  compute sub_collection_expanded     using collection_expanded vectors

   B. list graph                           determined by enumerated list membership
       4.  compute list_reduced                using members lists
       5.  compute list_expanded               using list_reduced vectors
       6.  compute sub_list_expanded           using list_expanded_vectors

   C. set graph                            deep union of graphs -- walking graph upward collecting ALL ancestors of BOTH col/list type
       7.  make set_reduced                    combining collection_reduced and list_reduced
       8.  compute set_expanded                using set_reduced vectors
       9.  compute sub_set_expanded            using set_expanded vectors

     * note that set expansion is N x MORE than the simple union of computed collection_expanded list_expanded!

   FOR ALL ITEMS,
    ensure we have list_reduced and collection_reduced (for runtime expansion case)

   See Listerine.inc for API 
