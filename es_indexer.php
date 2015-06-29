#!/usr/bin/env php
<?
// Copyright(c)2005-(c)2015 Internet Archive. Software license GPL version 2.

/* require_once '/petabox/setup.inc'; */
require_once '/home/ximm/petabox/setup.inc';                    //   must use schema in petabox/etc/solr/conf with our fields!!!
require_once 'Console/CommandLine.php'; // um, part of PEAR

require_once '/home/ximm/projects/search/listerine/ESIndexer.inc';
require_once '/home/ximm/projects/search/listerine/Listerine.inc';

// Elasticsearch linedump indexer
//
// Read a line dump from collections graph processing, post the item with provided calculated fields to an ES index
//   relies on  ESIndexer.php::get_custom_collections()
// cannabalized from se_indexer.php, with queue interactions stripped out and replaced by read of JSON linedump

// NOTE
//      this indexzer is intended for bootstrapping an index only; queue must be used once index is up

// TODO:
//      if --postlines exceeds available lines, process never quits
//      abstract line parsing, so can be used with any line-dump based single-shot indexing task


Util::memory_limit('4000M'); // may get big!
$GLOBALS['fatal-exceptions']=1;

const QLIMIT = 40000;

$pause_before_exit_seconds = 0;

// Catch fatal errors (such as (null)->fncall() within a library.)
//
// In our case, we just want to pause before exiting (if --pause was
// supplied) so that upstart doesn't give up on us.

register_shutdown_function("fatal_handler");

function fatal_handler()
{
  global $pause_before_exit_seconds;
  sleep( $pause_before_exit_seconds * (1 + (mt_rand() / mt_getrandmax()) * .5));
}


class IndexerHelper
{

  const ITEMS_LINE_DUMP = "/1/ximm_tmp/cut/linedump-cleaned_combined.json";
  const POST_BATCH_SIZE = 1000;
  
  public function __construct( $dump_path=false, $ptr=false, $post=true )
  {
    $this->IGNORES = (
      // tasks that are not tasks for *actual* items (eg: rescue tasks):
      "'".join("','",Util::cmd("cd /petabox/sw/work  &&  fgrep realItem *.php | fgrep -v fixer.php | perl -ne 'print if m/realItem\s*=\s*false/' | cut -f1 -d: | sort -u", "ARRAY","QUIET"))."'".
      // we dont update SE for bup.php tasks
      ",'bup.php'");
    /* error_log("LIST OF TASKS TO IGNORE: $this->IGNORES"); */

    $this->post_doc = $post;
    
    
    if ($dump_path) {
        $this->spl_file_object = new SplFileObject( $dump_path );
        $this->spl_file_ptr_path = $dump_path . ".ptr";        
    } else {
        $this->spl_file_object = new SplFileObject( self::ITEMS_LINE_DUMP );
        $this->spl_file_ptr_path = self::ITEMS_LINE_DUMP . ".ptr";
    }

    $this->stop_file_name = dirname( $dump_path ) . '/' . 'ES_INDEXER_STOP' ;
    $this->pause_file_name = dirname( $dump_path ) . '/' . 'ES_INDEXER_PAUSE' ;

    $this->sleep_file_name = dirname( $dump_path ) . '/' . 'ES_INDEXER_SLEEP' ;
    $this->intrasleep_file_name = dirname( $dump_path ) . '/' . 'ES_INDEXER_INTRASLEEP' ;
    
    if ($ptr) {
        $this->spl_file_ptr = $ptr;    
    } else { 
        $this->spl_file_ptr = $this->read_ptr();
    }
        
    
  }

  private function cache_ptr()
  {    
    echo "Updating $this->spl_file_ptr_path\n";
    $ptr_str = strval( $this->spl_file_ptr ) . "\n";
    file_put_contents( $this->spl_file_ptr_path, $ptr_str );
  }
  
  private function read_ptr()
  {
        if (file_exists( $this->spl_file_ptr_path )) {
            $ptr_str = trim( file_get_contents( $this->spl_file_ptr_path ));
            return intval( $ptr_str );
        } else {
            return 0;
        }
  }

  // Pull $total_to_post items from file and index them in $batch_size batches
  // Optionally sleep between batches if magic control file appears in source file directory
  // Pause or stop indexing if magic control files appear in source file directory 
  
  public function post_lines( $total_to_post, $batch_size = 1001 )
  {

    $posted_so_far = 0;

    $stop_now = FALSE;

    $intra_request_sleep = 5000;

    if ( file_exists( $this->intrasleep_file_name ) ) {
                
        try {
            $sleep_contents = file_get_contents( $this->intrasleep_file_name );
            if ( isset( $sleep_contents ) && ( strlen( $sleep_contents ) > 0 ) )
                $intra_request_sleep = min( 1000000, intval( $sleep_contents ));
        }
        catch (Exception $e) {
            ;
        }

    }

    
    while (( $posted_so_far < $total_to_post ) && ( $stop_now == FALSE ) )
    {      

        $left_to_post = $total_to_post - $posted_so_far;

        $to_post_this_time = min( $batch_size, $left_to_post );
            
        if (self::post_batch( $to_post_this_time, $intra_request_sleep ))
            $posted_so_far += $to_post_this_time;

        while ( file_exists( $this->pause_file_name ) &&  (! file_exists( $this->stop_file_name )) )
            sleep ( 5 );

        if ( file_exists( $this->sleep_file_name ) &&  (! file_exists( $this->stop_file_name )) ) {
            
            $sleep_secs = 1;
            
            try {
                $sleep_contents = file_get_contents( $this->sleep_file_name );
                if ( isset( $sleep_contents ) && ( strlen( $sleep_contents ) > 0 ) )
                    $sleep_secs = min( 120, intval( $sleep_contents ));
            }
            catch (Exception $e) {
                ;
            }

            sleep( $sleep_secs );
        }
        
        $stop_now = file_exists( $this->stop_file_name );
        
    }
    
    $success = ( $posted_so_far >= $total_to_post );
    
    return $success;    
  
  }

  
  // Pull $batch_size items from file and index them.

  private function post_batch( $batch_size, $intra_request_sleep=5000 )
  {

    $updated = false;

    $ids = Array();
    $line_hints = Array();

    $this->spl_file_object->seek( $this->spl_file_ptr );       
    if (!$this->spl_file_object->eof()) {
    
        $hint_ln = $this->spl_file_object->current();
        $this->spl_file_ptr += 1;
        
        if ( (! isset($hint_ln)) || $hint_ln=='' )
            continue;

        $left = max( 0, $batch_size - 1);
            
        $hla = json_decode( $hint_ln, TRUE );
        if ( isset( $hla )  ) {
            if (isset($hla['id'])) {
                $ids[] = $hla['id'];
                $line_hints[ $hla['id'] ] = trim( $hint_ln );
            }
        } else {
            $maybe = trim( $hint_ln );
            if ( isset( $maybe ) && ( $maybe !='' ) )
                $ids[] = $maybe;
        }
        
        while (!$this->spl_file_object->eof() && ($left > 0) ) {

            $hint_ln = $this->spl_file_object->fgets();

            $this->spl_file_ptr += 1;
            if ( (! isset($hint_ln)) || $hint_ln=='' )
                continue;            

            $left -= 1;

            $hla = json_decode( $hint_ln, TRUE );
            if ( isset( $hla )  ) {
                if (isset($hla['id'])) {
                    $ids[] = $hla['id'];
                    $line_hints[ $hla['id'] ] = trim( $hint_ln );
                }
            } else {
                $maybe = trim( $hint_ln );
                if ( isset( $maybe )  && ( $maybe !='' ) )
                    $ids[] = $maybe;
            }
        }    
    }

    if ( count( $ids ) ){
      $updated = true;
      error_log('updating for '.count($ids).' ids');
    }

    $finished = array();

    $notes = array();
    foreach ( $ids as $id )
    {
      $success = false;
      try
      {
        if ( isset( $line_hints[$id] ) )
          $ln_hint = $line_hints[ $id ];
        else
          $ln_hint = false;
                  
        $msg = ESIndexer::id2se(    $id,
                                    true,       // post if accumulated lines =...
                                    $batch_size,
                                    $success,
                                    $this->post_doc,
                                    $ln_hint );
                                    
        usleep( $intra_request_sleep );
                                    
      }
      catch (Exception $e) 
      {
        $msg = $e->getMessage();
        $success = false;
      }
      
      error_log("$id\t$msg");

      if ($success)
        $finished[] = $id;
      else
        $notes[$id] = $msg;
    }

    $this->cache_ptr();
    
    $post_errors = array();
    ESIndexer::post_flush($post_errors); // might fail (with exception)
    
    $finished = array_diff($finished, array_keys($post_errors));
    $notes = $notes + $post_errors;

    ESIndexer::flush_deletes();
  
    return $updated;
    
  }

 
}



function main()
{
  $parser = new Console_CommandLine();
  $parser->description = "Post from static list to ES.\n\n"
    . "To re-queue unfinished tasks from the end of the retry queue, do:\n"
    . "es_indexer.php --list-failures | cut -f-1 | tail -25 | es_indexer.php --ids=- --queue=0 --priority=5 --finish";
  $parser->add_version_option = false;
  $parser->name = 'es_indexer.php';

  $parser->addOption('dryrun', array(
    'long_name'   => '--dryrun',
    'description' => 'create a solr doc for the given id, and dump to stdout',
    'action'      => 'StoreTrue'
  ));

  $parser->addOption('postlines', array(
    'long_name'   => '--postlines',
    'help_name'   => 'N',
    'description' => 'read up to N lines from dumpfile and index them',
    'action'      => 'StoreInt'
  ));
  
  $parser->addOption('ldpath', array(
    'long_name'   => '--ldpath',
    'help_name'   => 'PATH',
    'description' => 'path for dump file',
    'action'      => 'StoreString',
  ));
  
  $parser->addOption('ldptr', array(
    'long_name'   => '--ldptr',
    'help_name'   => 'N',
    'description' => 'line pointer for dump file',
    'action'      => 'StoreInt',
  ));

  $parser->addOption('count', array(
    'long_name'   => '--count',
    'help_name'   => 'N',
    'description' => 'lines N to post',
    'action'      => 'StoreInt',
    'default'     => 5
  ));
  
  $parser->addOption('pause', array(
    'long_name'   => '--pause',
    'help_name'   => 'N',
    'description' => 'pause N seconds before exiting (+ 50% jitter)',
    'action'      => 'StoreInt',
    'default'     => 0
  ));
  $parser->addOption('loop', array(
    'long_name'   => '--loop',
    'description' => 'loop if lines remain',
    'action'      => 'StoreTrue'
  ));


  try
  {
    $c = $parser->parse();
  }
  catch (Exception $e)
  {
    $parser->displayError($e->getMessage());
  }
  $sawopt = false;
  foreach(array_values($c->options) as $val){
    if ($val !== null)
      $sawopt = true;
  }
  if (!$sawopt)
    $parser->displayUsage();

  Util::must_run_on('bigsearch0');
  
  if ($c->options['pause'])
  {
    global $pause_before_exit_seconds;
    $pause_before_exit_seconds = max(1,$c->options['pause']);
  }
  
  if ( $c->options['ldpath'] ) {
  
      if ( file_exists($c->options['ldpath']) == FALSE ) {

          error_log( "Source file does not exist " . $c->options['ldpath'] );

      } else {

          $dryrun =  isset($c->options['dryrun']);
  
          if ( isset( $c->options['ldptr'] ))
            $ptr = $c->options['ldptr'];
          else
            $ptr = false;

          $indexer = new IndexerHelper( $c->options['ldpath'], $ptr, $dryrun );

          $lines_posted = false;

          while (true)
          {

            if ($c->options['postlines'])
              $to_post = intval( $c->options['postlines'] );
              $lines_posted = $indexer->post_lines( $to_post, IndexerHelper::POST_BATCH_SIZE );

            if (!($lines_posted) || !$c->options['loop'])
              break;

          }
          // we don't explicitly pause here, as fatal_handler does it for us.
      }    
    }
  
  exit(0);
}

main();

// Working examples of using an external command....
// TODO: UPDATE FOR THIS VERSION

// php /home/ximm/projects/search/es_indexer.php --postlines=150000 --ldpath=/1/ximm_tmp/cut/linedump-cleaned_combined.json &>/1/ximm_tmp/cut/logs/linedump-cleaned_combined.log
// php /home/ximm/projects/search/es_indexer.php --postlines=1 --ldpath=/1/ximm_tmp/cut/donow &>/1/ximm_tmp/cut/logs/donow.log
 
/* (~/petabox/sw/bin/es_indexer.php --unqueue=500 --external-command='python /home/mccabe/s/archive/search/update_beta.py -' --loop) &> unq4& */
/* while `true`; do ~/petabox/sw/bin/es_indexer.php --queue=1000 --unqueue=500 --loop  --external-command='python /home/mccabe/s/archive/search/update_beta.py -' --pause=10; done */
