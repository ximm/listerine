#!/bin/python

# TODO:     consider making sub-collections and ancestors vectors include self to simplify ES DSL queries performed by Listerine
# TODO:     compute invalidations (reindexing lists)
# TODO:     possible optimization: caching all collection data in separate non-sharded index with copy on every node
#               * idea is to guarantee terms query can GET sub-set data on local node, lowering intra-cluster traffic


import sys
import os
import codecs
import json

import copy

#
#   RECIPE
#
#   NOTE:     indexing cleaned items relies on graph being fully indexed with sub-collection/list/set, items cannot be correctly indexed UNTIL INDEX IS POPULATED WITH GRAPH
#
#   1. Generate raw dump of all items with their collection and member vectors, e.g. from Elasticdump from an existing index, Metamgr, iamine, and/or users table (for membership)
#   2. Process COLLECTIONS:
#           derive collection_reduced and then collection_expanded vector (from original collection vector);
#           derive list_reduced then list_expanded by inverting members (from members.json); and
#           synthesize set_reduced and set_expanded as a recursive deep union of above (NOT a simple union of individual _expanded vectors!)
#      This script dumps results, one json dict per item per line, for subsequent indexer ingest (see es_indexer.php)
#   3. INDEX COLLECTIONS into a clean ES index. Only reduced and expanded set and collection matter; sub-collection/list/set fields CANNOT be calculated on first pass
#           * disable sub-collection queries in indexer for fast indexing (in ESIndexer.php).
#   4. REINDEX COLLECTIONS. This time, indexing can populate sub-collection/list/set fields (since _expanded values are now completely searchable in the index)
#   5. Process ITEMS, dumping one json dict per item per line, only relevant field is rcollection_reduced
#   6. INDEX ITEMS with collection_reduced from dump and calculate list_reduced using queries (into same ES index holding collection graph data)
#           * membership retrieved via Listerine which makes ES query in es_indexer
#   7. Listerine API can now answer questions about collection, list, and set membership in both immediate (reduced) and expanded cases, and produce facets
#
#   TERMINOLOGY
#
#   collection      item with mediatype=collection,                 graph truth computed from by parent vector
#   list            item with mediatype=collection, members.json,   graph truth inverted from members lists (currently: users table)
#   set             combination of above mediatype=collection       inclusive recursive deep union, accumulating all parents of BOTH types at EVERY ancestor
#       
#   FOR ALL COLLECTIONS, 
#    ensure we have information to
#
#   A. collection graph aspect              determined by ancestor relationships
#       1.  compute collection_reduced          using existing metadata, using graph reduction
#       2.  compute collection_expanded         using collection_reduced vectors
#       3.  compute sub_collection_expanded     using collection_expanded vectors
#
#   B. list graph                           determined by enumerated list membership
#       4.  compute list_reduced                using members lists
#       5.  compute list_expanded               using list_reduced vectors
#       6.  compute sub_list_expanded           using list_expanded_vectors
#
#   C. set graph                            deep union of graphs -- walking graph upward collecting ALL ancestors of BOTH col/list type
#       7.  make set_reduced                    combining collection_reduced and list_reduced
#       8.  compute set_expanded                using set_reduced vectors
#       9.  compute sub_set_expanded            using set_expanded vectors
#
#     * note that set expansion is N x MORE than the simple union of computed collection_expanded list_expanded!
#
#   D. item output for indexing
#       10. should contain ALL fields above, plus native members and original collections vector


# 1. raw output of elasticdump

elasticdump_collections_output =    "collection_dump.json"           # all items of mediatype collection (NB: there are a few missing malformed items with missing members.json)
elasticdump_items_output =          "item_dump.json"                 # all items NOT of mediatype collection

failed_extension                    = ".procfail" 

# 3. combined export of cleaned graph nodes
cleaned_combined_linedump           = "linedump-cleaned_combined.json" 

# 4. subsequent output of cleaned items
cleaned_items_linedump              = "linedump-cleaned_items.json" 


class cgraph:

    def __init__( self, cnodes={} ):

        self.cnodes = cnodes

    def get_nodes( self ):
        return self.cnodes.values()

    def get_node_count( self ):
        return len( self.cnodes )

    def has_node_named( self, cname ):
        return cname in self.cnodes

    def get_node_named( self, cname ):
        if self.has_node_named( cname ):
            return self.cnodes[ cname ]
        else:
            return None

    def add_node( self, name, orig_vector=None ):
        node = cnode( self, name, orig_vector )        
        self.cnodes[ node.name ] = node
        return node

    def remove_node( self, cnode ):
        self.cnodes[ cnode.name ] = cnode

    def simplify( self ):
        '''Pass through all constituent nodes which do not have collection vector fully reduced, or members fully inverted, and see if they can simplify'''

        broken_items = {}

        # computes parents from original collection vector
        for name,node in self.cnodes.iteritems():
            if node.get_accurately_reduced_parents() is True:
                continue
            ( accurately_reduced_parents, reason ) = node.reduce_collection_parents()
            if not accurately_reduced_parents:
                broken_items[ name ] = reason

        # computes list_parents by inverting members
        for name,node in self.cnodes.iteritems():
            if node.get_accurately_inverted_members() is True:
                continue
            ( accurately_inverted_members, reason ) = node.invert_members()
            if not accurately_inverted_members:
                broken_items[ name ] = reason

        # computes

        return broken_items


    # serialization

    def export_linedump( self, linedump_filename ):

        linedump_filename_failed = linedump_filename + failed_extension
        print "Writing graph dump to %s, errors to %s..." %  ( linedump_filename, linedump_filename_failed )

        with codecs.open( linedump_filename, encoding='utf-8', mode='w' ) as write_handle:
            with codecs.open( linedump_filename_failed, encoding='utf-8', mode='w' ) as write_handle_failed:
                for name,node in self.cnodes.iteritems():
                    ln = node.line_dump()
                    write_handle.write( ln + '\n' )
                    # also write failures to separate err file
                    if node.get_accurately_reduced() is False:
                        write_handle_failed.write( ln + '\n' )

         
    def import_linedump( self, linedump_filename, simplified=False ):
        '''Import a collections linedump, build each line into a node, which requires subsequent reduction'''        

        if simplified:

            # output after graph simplification, in cgraph.export_linedump()

            with codecs.open( linedump_filename, encoding='utf-8', mode='r' ) as read_handle:
                for rln in read_handle:
                    ln = rln.strip()
                    try:
                        if ln[0] == "{":
                            res = json.loads( ln )
                        else:
                            badline
                    except:
                        print "Skipping %s" % ln
                        continue                    

                    identifier                              = res[ 'id' ]
                    original_collection_vector              = res[ 'collection_original' ]                  
                    
                    if self.has_node_named( identifier ):
                        node = self.get_node_named( identifier )
                        node.set_original_collection_vector( original_collection_vector )
                    else:                
                        node = self.add_node( identifier, original_collection_vector )


                    node.set_collection_parents(            res[ 'collection_reduced' ] )
                    node.set_list_parents(                  res[ 'list_reduced' ] )

                    node.set_original_members(              res[ 'members_original' ] )
                                        
                    node.set_accurately_reduced_parents(    res[ 'accurately_reduced_parents' ] )
                    node.set_accurately_inverted_members(   res[ 'accurately_inverted_members' ] )

            # now rehydrate fields we store as actual nodes
            for name,node in self.cnodes.iteritems():
                hydrated_parents = [ self.get_node_named( n ) for n in node.get_collection_parents() ]
                node.set_collection_parents( hydrated_parents )                
                hydrated_parents = [ self.get_node_named( n ) for n in node.get_list_parents() ]
                node.set_list_parents( hydrated_parents )

        else:

            # raw dump from elasticdump, see DUMP_LIST_DATA.sh
            
            with codecs.open( linedump_filename, encoding='utf-8', mode='r' ) as read_handle:
                for rln in read_handle:
                    ln = rln.strip()
                    try:
                        if ln[0] == ",":
                            res = json.loads( ln[1:] )
                        elif ln[0] == "{":
                            res = json.loads( ln )
                        else:
                            badline
                    except:
                        print "Skipping %s" % ln
                        continue            

                    identifier          = res["_id"]
                    src                 = res["_source"]

                    exclude_me = False

                    if "mediatype" not in src.keys():
                        print "NO MEDIA TYPE? Skipping %s" % ln
                        continue            
                    else:                
                        media_type  = src["mediatype"]
                    if is_list( media_type ):
                        if len( media_type) == 1:
                            media_type = media_type[0]
                        else:
                            print "BAD MEDIA TYPE? Skipping %s" % ln
                            continue            

                    if "collection" not in src.keys():
                        original_collection_vector = []                    
                    else:    
                        original_collection_vector = src["collection"]
                        if is_list( original_collection_vector ) is False:
                            original_collection_vector = [ original_collection_vector ]
                        
                    if "members" not in src.keys():
                        original_members_vector = []                    
                    else:    
                        original_members_vector = src["members"]
                        if is_list( original_members_vector ) is False:
                            original_members_vector = [ original_members_vector ]                    

                    filtered_members_vector = [ m for m in original_members_vector if is_favorite_list( m ) ]

                    if self.has_node_named( identifier ):
                        node = self.get_node_named( identifier )
                        node.set_original_collection_vector( original_collection_vector )
                    else:                
                        node = self.add_node( identifier, original_collection_vector )

                    node.set_original_members( original_members_vector )
                    node.set_filtered_members( filtered_members_vector )


            # now compute filtered vector for each node
            # filtered means, of type collection: ie those nodes we have, or, other favorites lists

            for name,node in self.cnodes.iteritems():            
                filtered_members_vector = [ m for m in node.get_original_members() if ( is_favorite_list( m ) or self.has_node_named( m )) ]
                node.set_filtered_members( filtered_members_vector )
                    

# NOTE: some node references in sets are text strings corresponding to the node.name of other nodes, nodes DO NOT collect arrays of other cnode objects

class cnode:

    # original_collection_vector        a set of text strings, naming other nodes, unreduced
    # original_members                  a set of text strings, naming other nodes, unfiltered
        
    # collection_parents                a set of other cnodes (which should also be in the graph)
    # primary_collection_parent         a cnode (which should also be in the graph) , the single proper parent

    # list_parents                      a set of other cnodes (which should also be in the graph)
    
    # filtered_members                  a set of text strings, naming other nodes, inverted into list-parents
    
    
    def __init__( self, graph, name, original_collection_vector=None ):

        self.name = name
        self.graph = graph

        self.original_collection_vector = original_collection_vector
        self.original_members = set([])

        self.filtered_members = set([])

        self.primary_collection_parent = None
        self.collection_parents = set([])

        self.list_parents = set([])

        self.accurately_reduced_parents = False
        self.accurately_inverted_members = False        


    # accessors

    def get_graph( self ):
        return self.graph
        
    def get_name( self ):
        return self.name


    # from collection vector

    def get_original_collection_vector( self ):
        return self.original_collection_vector

    def set_original_collection_vector( self, v ):
        self.original_collection_vector = v

    def set_primary_collection_parent( self, node ):
        self.primary_collection_parent = node

    def get_primary_collection_parent( self):
        return self.primary_collection_parent

    def set_collection_parents( self, plist ):
        self.collection_parents = set( plist )
        
    def get_collection_parents( self ):
        return self.collection_parents

    def add_parent( self, p ):
        if self.primary_collection_parent is None:
            self.primary_collection_parent = p
        self.collection_parents.add( p )
        return self.collection_parents

    def remove_parent( self, p ):
        if self.prime_parent == p:
            self.prime_parent = None
        if p in self.collection_parents:
            self.collection_parents.remove( p )
        return self.collection_parents

    def get_accurately_reduced_parents( self ):
        if self.accurately_reduced_parents is True:
            return True
        else:
            return False

    def set_accurately_reduced_parents( self, val ):
        self.accurately_reduced_parents = val

    # from members vector

    def set_original_members( self, clist ):
        self.original_members = set( clist )

    def get_original_members( self ):
        return self.original_members


    def set_filtered_members( self, clist ):
        self.filtered_members = set( clist )

    def get_filtered_members( self ):
        return self.filtered_members


    def set_list_parents( self, plist ):
        self.list_parents = set( plist )

    def get_list_parents( self ):
        return self.list_parents

    def add_list_parent( self, p ):    
        self.list_parents.add( p )
        return self.list_parents

    def remove_list_parents( self, p ):
        if p in self.list_parents:
            self.list_parents.remove( p )
        return self.list_parents


    def get_accurately_inverted_members( self ):
        if self.accurately_inverted_members is True:
            return True
        else:
            return False

    def set_accurately_inverted_members( self, val ):
        self.accurately_inverted_members = val

    def get_accurately_reduced( self ):
        return self.get_accurately_reduced_parents() and self.get_accurately_inverted_members()


    # serialization



    def line_dump( self, all_fields=True ):

        dump = {}

        dump[ 'id' ]                            = self.get_name()
        
        dump[ 'collection_original' ]           = self.get_original_collection_vector()
        dump[ 'collection_reduced' ]            = list( self.collection_parents_list() )
        dump[ 'collection_expanded' ]           = list( self.collection_parents_list( True ) )

        dump[ 'accurately_reduced_parents' ]    = self.get_accurately_reduced_parents()

        if all_fields:

            dump[ 'members_original' ]              = list( self.get_original_members() )

            dump[ 'list_reduced' ]                  = list( self.list_parents_list() )
            dump[ 'list_expanded' ]                 = list( self.list_parents_list( True ) )
                    
            dump[ 'set_reduced' ]                   = list( self.set_list() )
            dump[ 'set_expanded' ]                  = list( self.set_list( True ) )
                    
            dump[ 'accurately_inverted_members' ]   = self.get_accurately_inverted_members()

        ln = json.dumps( dump )
            
        return ln

    
    
    # graph operations
    
    
    def collection_parents_list( self, expanded=False ):
        '''return list of text names, not nodes, with primary parent always first if there is one'''
        if expanded:
            l = list( [ p.get_name() for p in self.get_collection_ancestors() ] )
        else:
            l = list( [ p.get_name() for p in self.get_collection_parents() ] )
        if self.primary_collection_parent is not None:
            ppn = self.primary_collection_parent.get_name()
            if ppn in l:
                l.remove( ppn )
#            else:
#                print 'WTH!', self.get_name(), ppn, json.dumps( l ), 
            l.insert( 0, ppn )
        return l

    def list_parents_list( self, expanded=False ):
        '''return list of text names, not nodes'''
        if expanded:
            l = list( [ p.get_name() for p in self.get_list_ancestors() ] )
        else:
            l = list( [ p.get_name() for p in self.get_list_parents() ] )
        return l

    def set_list( self, expanded=False ):
        '''return list of text names, not nodes'''
        if expanded:
            l = list( [ p.get_name() for p in self.get_set_ancestors() ] )
        else:
            l = list( [ p.get_name() for p in self.get_set() ] )
        return l



    def filtered_members_list( self ):
        '''return list of text names, not nodes'''
        l = list( [ p.get_name() for p in self.get_filtered_members() ] )
        return l


    def get_collection_ancestors( self ):
        ancestors = self.compute_collection_ancestry()
        ancestors.remove( self )
        return ancestors        
    
    def compute_collection_ancestry( self ):
        '''recursively collect parents'''
        ancestors = set([] )    
        ancestors.add( self )
        for p in self.get_collection_parents():
            ancestors = ancestors.union( p.compute_collection_ancestry() )
        return ancestors
            
    def reduce_collection_parents( self ):
        '''Yo this is where the magic happens'''
        
        # INPUT
        #   self.original_collection_vector        array of strings naming parents
        
        # OUTPUT
        # sets, and returns, 3-tuple:
        #   self.accurately_reduced_parents     boolean
        #   self.primary_collection_parent                 node or None
        #   self.collection_parents                        set, empty allowed

        accurately_reduced_parents = False
        reason = ''
        
        prime_parent_name = None
        prime_parent_node = None
        true_parent_nodes = []
    
        if len( self.original_collection_vector ) == 0:
    
            accurately_reduced_parents = True
        
        else: 

            ancestors_all_reduced = True
        
            exclude_set = set([])
        
            for ancestor in self.original_collection_vector:
                if self.name == ancestor:
                    print "WARNING: %s names itself in collection vector" % self.name
                else:
                    if self.graph.has_node_named( ancestor ):
                        ancestor_node = self.graph.get_node_named( ancestor )
                        if prime_parent_node is None:
                            prime_parent_node = ancestor_node                        
                        if ancestor_node not in exclude_set:
                            if ancestor_node.get_accurately_reduced_parents():
                                true_parent_nodes.append( ancestor_node )
                                exclude_ancestors = ancestor_node.get_collection_ancestors()
                                exclude_set = exclude_set.union( exclude_ancestors )
                            else:
                                reason = "MISSING ANCESTOR: while processing %s, could not find node %s" % (self.name, ancestor )                    
                                ancestors_all_reduced = False
                    else:
                        reason = "MISSING ANCESTOR: while processing %s, could not find node %s" % (self.name, ancestor )
                        ancestors_all_reduced = False

            if prime_parent_node is not None and ancestors_all_reduced is True:
                accurately_reduced_parents = True

        self.set_primary_collection_parent( prime_parent_node )
        self.set_collection_parents( true_parent_nodes )
        self.set_accurately_reduced_parents( accurately_reduced_parents )

#        if not accurately_reduced_parents:
#            print reason
            
        return ( accurately_reduced_parents, reason )


    #
    # LIST PARENTS
    #

    # for collections which have members, which need to be distributed to other collections
    def invert_members( self ):
        
        # INPUT
        #   self.filtered_members        enumeration of collections/lists for whom this node should be recorded as a list_parent
        
        # OUTPUT
        #   <nodes 1-N>.list_parents     updates list_parents of OTHER nodes
            
        # drain filtered

        accurately_inverted_members = True
        reason = ''
        
        missing_members = []
        
        for member_name in self.get_filtered_members():
        
            if self.graph.has_node_named( member_name ):
                node_needing_parent = self.graph.get_node_named( member_name )
                node_needing_parent.add_list_parent( self )
#                print "adding %s as parent to %s" % (  self.get_name(), node_needing_parent.get_name() )
            else:
                missing_members.append( member_name )

        if len( missing_members ) > 0:
            accurately_inverted_members = False
            reason = 'UNKNOWN MEMBERS: while processing %s, could not find members %s' % (self.name, ', '.join(missing_members) )

        self.set_accurately_inverted_members( accurately_inverted_members )

#        if not accurately_inverted_members:
#            print reason
            
        return ( accurately_inverted_members, reason )        
        
    def get_list_ancestors( self ):
        ancestors = self.compute_list_ancestry()
        ancestors.remove( self )
        return ancestors        

    def compute_list_ancestry( self ):
        '''recursively collect list_parents'''
        ancestors = set([] )    
        ancestors.add( self )
        for p in self.get_list_parents():
            if p not in ancestors and is_favorite_list( p.get_name() ):
                ancestors = ancestors.union( p.compute_list_ancestry() )
        return ancestors


    #
    #   SETS combining collection and list
    #

    def get_set_ancestors( self ):
        ancestors = self.compute_set_ancestry()
        ancestors.remove( self )
        return ancestors        

    def compute_set_ancestry( self ):
        '''recursively collect list_parents'''
        ancestors = set([] )    
        ancestors.add( self )
        for p in self.get_set():
            if p not in ancestors:
                ancestors = ancestors.union( p.compute_set_ancestry() )
        return ancestors

    def get_set( self ):
        combined_set = list( self.get_collection_parents() ) + list( self.get_list_parents() )
        return combined_set

# miscellaneous utility


def is_favorite_list(v):
    return (len(v) > 4) and ( v[0:4] == "fav-" )

def is_list(v):
   return ( hasattr(v, '__iter__') and not isinstance(v, basestring) )

def same_members(a, b):
    return set(a) == set(b)
    
def union(a, b):
    return list(set(a) | set(b))

def file_exists ( path ):
    return os.access( path, os.F_OK)

def write_dict( dict, fn ):

    print "...writing %s" % fn

    with codecs.open( fn, encoding='utf-8', mode='w' ) as fh:
        fh.write( "{\n" )
        for k,v in dict.iteritems():
            vs = json.dumps( v, indent=4, separators=(',', ': ') )
            fh.write( '"%s": %s,\n' % (k, vs) )
        fh.write( '"__foo":[] \n')
        fh.write( "}")

           
    

#
#   GRAPH
#        



def build_graph( ):

    print "Importing nodes..."

    graph = cgraph()
    
    if file_exists( cleaned_combined_linedump ):
    
        graph.import_linedump( cleaned_combined_linedump, simplified=True )
    
    elif file_exists( elasticdump_collections_output ):
    
        graph.import_linedump( elasticdump_collections_output )
    
    else:
    
        print "ABORT: cannot build graph, neither %s (nor %s and %s) to import!" % ( cleaned_combined_linedump, collections_binned_linedump, lists_filtered_linedump )
        return


    print "Constructing graph..."

    pass_number = 1
    last_unreduced_node_count = -1
    keep_going = True
                    
    while keep_going is True:

        print "Pass %s..." % pass_number
                
        unreduced_nodes = graph.simplify()
        unreduced_node_count = len( unreduced_nodes )
        
        print "\tunreduced nodes this pass: %s" % unreduced_node_count

        if unreduced_node_count == 0 or unreduced_node_count == last_unreduced_node_count:
            keep_going = False

        last_unreduced_node_count = unreduced_node_count
        pass_number += 1

    if True:
        print "Reduced node summary"
        print "----------------------"
        print '%s nodes' % ( graph.get_node_count() - len( unreduced_nodes ) )

        print "\n"
        print "Unreduced node summary"
        print "----------------------"
        print '%s nodes' % len( unreduced_nodes )

        for node_name,reason in unreduced_nodes.iteritems():
            print "%s\t\t%s" % (node_name, reason )

        print "\n"

    if file_exists( cleaned_combined_linedump ) is False:
        graph.export_linedump( cleaned_combined_linedump )

    return graph
    



        
def clean_items( graph ):
    '''Parse non-collection items' collections vectors and reduce them against known graph'''
    
    if file_exists( cleaned_items_linedump ):
        print "Skipping cleaned items dump, %s exists..." % cleaned_items_linedump
        return

    if file_exists( elasticdump_items_output ) is False:
        print "ABORT: need input file %s" % elasticdump_items_output
        return

    if graph is None:
        print "ABORT: need graph"
        return
        
    print "Parsing %s..." % elasticdump_items_output
    
    nodes = {}

    with codecs.open( cleaned_items_linedump, encoding='utf-8', mode='w' ) as write_handle:
    
        with codecs.open( elasticdump_items_output, encoding='utf-8', mode='r' ) as read_handle:
        
            for rln in read_handle:
                ln = rln.strip()
                try:
                    if ln[0] == ",":
                        res = json.loads( ln[1:] )
                    else:
                        res = json.loads( ln )
                except:
                    print "* SKIPPING %s" % ln
                    continue            

                identifier  = res["_id"]
                src         = res["_source"]

                # print src

                if "mediatype" not in src.keys():
                    try:
                        print "* NO MEDIA TYPE? Skipping %s" % ln
                    except:
                        print "* NO MEDIA TYPE? Skipping item [could not print]"
                    continue            
                else:                
                    media_type  = src["mediatype"]
                if is_list( media_type ):
                    if len( media_type) == 1:
                        media_type = media_type[0]
                    else:
                        try:
                            print "* BAD MEDIA TYPE? Skipping %s" % ln
                        except:
                            print "* BAD MEDIA TYPE? Skipping [could not print]"
                        continue

                if "collection" not in src.keys():
                    collections_vector = []                    
                else:    
                    collections_vector = src["collection"]
                    if is_list( collections_vector ) is False:
                        collections_vector = [ collections_vector ]

                if media_type == "collection":
                    continue

                # create node and reduce native collection parent
                # NOTE: list and set membership can only be gleaned by inquiring fully-populated index, MUST BE handled by indexer currently
                node = cnode( graph, identifier, collections_vector )
                ( accurately_reduced, reason ) = node.reduce_collection_parents()
                ln = node.line_dump( False )
                write_handle.write( ln + '\n' )                    

                if not accurately_reduced:
                    try:
                        print "* COULD NOT REDUCE: %s (%s)" % ( identifier, reason )                    
                    except:
                        print "* COULD NOT REDUCE: [could not print]"                    



def main(argv=None):
    

    # construct a graph and dump of distilled collections vectors
    graph = build_graph()

    # process non-collection items against collections graph
    clean_items( graph )

            

if __name__ == "__main__":
    sys.exit(main())
    
    
    
    
# USING ELASTICDUMP TO GET COLLECTIONS SUBSETS

# see also DUMP_LIST_DATA.sh 

    # ximm@collections:~/projects/search/lists$ time elasticdump --maxSockets=1000 --input=http://es-lb.archive.org:9200/archive-all-2 --output=$ --limit=10000 --searchBody '{ "_source": ["collection","mediatype"], "query" : { "match_all": {} } }' | pv -acbrl > collections_dump.json
    #  21.8M [8.69k/s] [8.69k/s]
    # 
    # real    41m54.064s
    # user    20m56.719s
    # sys     7m18.887s

    # ximm@collections:~/projects/search/lists$ time elasticdump --maxSockets=1000 --input=http://es-lb.archive.org:9200/archive-all-2 --output=$ --limit=10000 --searchBody '{ "_source": ["collection","mediatype","members"], "query" : { "match_all": {} } }' | pv -acbrl > combined_dump.json


    # UNTIL THERE ARE OTHER KINDS OF LISTS THIS CHEAT WORKS:
    #  * real test should be, all items with a non-empty members list
    # ximm@collections:~/projects/search/lists$ time elasticdump --maxSockets=1000 --input=http://es-lb.archive.org:9200/archive-all-2 --output=$ --limit=10000 --searchBody '{ "_source": ["members","mediatype","collection"], "filter" : { "prefix": { "_id":"fav-"} } }' | pv -acbrl > favorites_dump.json
    #  122k [ 650/s] [ 650/s]]
    # 
    # real    3m7.180s
    # user    0m8.185s
    # sys     0m2.696s
    