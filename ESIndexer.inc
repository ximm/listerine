<?
// Copyright(c)2005-(c)2015 Internet Archive. Software license GPL version 2.

define('DEFAULT_GROUP_SIZE', 500);

define('SE_PRINT_DONT_POST', isset($_SERVER['SE_PRINT_DONT_POST'])); // DEBUG mode or not! (set on cmd-line env before running CLI, etc.)
#define('SE_PRINT_DONT_POST', TRUE );

/*   
    Current docuyment processing path flows through

    ESIndexer::xslt()
        
        $xsl  = "$root/www/sf/includes/lc-names.xsl";
        $xsl2 = "$root/www/sf/includes/index.xsl";
    
        <xsl:variable name="schema" select="document('../../../etc/solr/conf/schema.xml.homenodes-archive.org')/schema"/>

*/

class ESIndexer
{
  
  const MAX_FAVORITES = 10000;
  const ITEMS_LINE_DUMP = "/1/ximm_tmp/linedump-cleaned_combined.json";

  public static $fields_of_interest = array(    'collection_reduced',
                                                'collection_expanded',
                                                'list_reduced',
                                                'list_expanded',
                                                'set_reduced',
                                                'set_expanded',
                                                'accurately_reduced_parents',
                                                'accurately_inverted_members' );


  public static $fields_of_interest_item = array(   'collection_reduced',
                                                    'accurately_reduced_parents', );

  public static function get_custom_collections( $id, $ln_hint=false, $is_collection=false )
  {

    if ($ln_hint)
        $ln = $ln_hint;
    else
        $ln = Util::cmd( 'grep "id": "' . $id .'" ' . self::ITEMS_LINE_DUMP );
    
    $provided_value_dict = json_decode( $ln, TRUE );

    $lists = Array();
        
    if ($is_collection) {

        foreach ( $provided_value_dict as $k => $v )
            if ( in_array($k, ESIndexer::$fields_of_interest ))
                if ( is_bool($v) )
                  $lists[ $k ] = strval( $v );
                else
                  $lists[ $k ] = $v;  

        // TODO: add FLAG HERE TO DISABLE ATTEMPTED EXPANSION ON FIRST PASS
        $lists[ 'sub_collection_expanded' ]     = Listerine::expand_subcollections( $id, Listerine::TYPE_COLLECTION );
        $lists[ 'sub_list_expanded' ]           = Listerine::expand_subcollections( $id, Listerine::TYPE_LIST );   
        $lists[ 'sub_set_expanded' ]            = Listerine::expand_subcollections( $id, Listerine::TYPE_SET );

    } else {

        foreach ( $provided_value_dict as $k => $v )
            if ( in_array($k, ESIndexer::$fields_of_interest_items ))
                if ( is_bool($v) )
                  $lists[ $k ] = strval( $v );
                else
                  $lists[ $k ] = $v;

        $lists[ 'list_reduced' ]                = Listerine::get_lists_for_item( $id, false );        
        $lists[ 'set_reduced' ]                 = array_merge( $lists[ 'collection_reduced' ], $lists[ 'list_reduced' ] );

    }

//    var_dump( $lists );
    
    return $lists;
    
  }
  
  // checks to see if an item is in the search engine
  public static function itemExists($identifier)
  {
    $s=new Search(array('query'=>"identifier:$identifier", 'limit'=>0));
    return (int)($s->getInfo("total_nbr"));
  }

  // name of ES index to operate on (note: queries via aliases)
  private static $es_index = 'list-test';

  private static $post_xml='';
  private static $post_num=0;
  private static $post_total=0;
  private static $post_start_time=0;
  private static $post_group_size=DEFAULT_GROUP_SIZE;
  private static $ids_to_delete = array();

  

  public static function id2se($id, $post=false, $group_size=DEFAULT_GROUP_SIZE, &$success=false, $return_doc=false, $ln_hint=false )
  {

#    echo $id . "\n";
#    echo $ln_hint. "\n";
    
    $GLOBALS['fatal-exceptions']=1;
    try 
    {
      self::$post_group_size = $group_size;
      
      $last_task_was_delete = false;

      $local_md = FALSE;
      
      if ( $local_md ) {

          $m = Metadata::get_obj($id, array('authed' => true,
                                            'files_limit' => 1000,
                                            'get_timeout' => 1,
                                            'dontcache' => true));
      } else {

          $murl = "http://archive.org/metadata/" . $id ;
          $murl .= "?dontcache";
      
          $mj = file_get_contents( $murl );
          $m = json_decode( $mj, 1 );
      
      }
/*                                        
      $murl = "http://archive.org/metadata/" . $id ;
      $murl .= "?dontcache";
      
      $mj = file_get_contents( $murl );
      $m = json_decode( $mj, 1 );
*/
      
      if (count($m) === 0 || isset($m['error'])){
        $tasks = DB::qrows("SELECT cmd FROM catalog_done WHERE", array("identifier" => $id),
                           "ORDER BY finished DESC LIMIT 1");
        if (count($tasks) && $tasks[0]['cmd'] == 'delete.php'){
          $last_task_was_delete = true;
        }
        else{
          $error = isset($m['error']) ? ": " . $m['error'] : "";
          return "item metadata for $id not retrievable" . $error;
        }
      }
      

      $delme = ($last_task_was_delete  ||  @$m['is_dark']);
     
      if (isset($m['is_dark'])) // belt + suspenders???
        $delme = true;
 
      if ($delme){
        // doing a SE delete!
        self::queue_delete($id);
        /**/ if (@$m['is_dark'])                        $reason = "API as is_dark";
        else if ($last_task_was_delete)                 $reason = "delete task already ran";
        else                                            $reason = "[bad logic]";

        $success = true;
        return "item deleted from SE ($reason)";
      }



      //printr($m);
      $xml='<?xml version="1.0" encoding="utf-8"?><results dir="'.$m['dir'].'" server="'.$m['server'].'">'."\n<metadata>\n";
      if (!isset($m['metadata']))
      {
        return "item has no <metadata>";
      }


      if ((!isset($m['metadata']['mediatype'])  ||  !isset($m['metadata']['collection']))  &&
          !$m['metadata']['mediatype']=='collection') {
        $success = true;
        return "doesn't have both mediatype and collection set - not indexing";
      }

      $languageSorted=false;
      
      foreach ($m['metadata'] as $key => &$vals){
        if (!is_array($vals))
          $vals = array($vals);

        // this field is actually used in _reviews.xml -- finding it in _meta.xml
        // is stray and should be omitted from SE indexing
        if ($key=='createdate')
          continue;

        if ($key=='addeddate'){
          // in our schema, this is a singletons -- so make sure if an item has 2+ such
          // of this elem, we take the *min* date (and for the record, it's *weird* if 2+
          // of this elem -- i mean, what would sorting on that field even *mean*, then 8-p)
          $min = Util::minTimeArray($vals);
          $min = self::fixdate($min);
          if ($min === false)
            continue;
          $vals = array($min);
        }

        if (count($vals)){
          foreach ($vals as $val){
            if (!is_array($val))
              $vals2 = array($val);
            if (count($vals2)){
              foreach ($vals2 as $val2){
                if ($key=='date' | $key=='date_created'){
                  if (($val2 = self::fixdate($val2)) === false)
                    continue;
                }
                else if ($key=='year'){
                  if (strlen($val2)>4)
                    continue;
                }
                else if (!$languageSorted  &&  $key=='language'){
                  $lang = self::language_coalesce($val2);
                  if ($lang != 'skip'){
                    $xml .= "<languageSorter>".XML::enc(str_replace(array('<','>'),array('&lt;','&gt;'),$lang))."</languageSorter>\n";
                    $languageSorted=true;//just doing first "good" one
                  }
                }

                $xml .= "<$key>".XML::enc(str_replace(array('<','>'),array('&lt;','&gt;'),$val2))."</$key>\n";
              }
            }
          }
        }
      }

      // Add contents of 'members'
      $members=array();
      if (isset($m['members'])) {
        foreach ($m['members'] as $member)
        {
          if (!is_array($member))
          {
            $xml .= "<members>$member</members>\n";
            $members[$member]=1;
          }
          else if (isset($member['identifier'])
                   && !(isset($member['mediatype'])
                        && $member['mediatype'] == 'search'))
          {
            $xml .= "<members>" . $member['identifier'] . "</members>\n";
            $members[$member['identifier']]=1;
          }
        }
      }

      // Insert customized collections vectors for collections (including lists) only
      if ( $m['metadata']['mediatype'] == 'collection' )
          $is_collection = TRUE;
      elseif ( is_array($m['metadata']['mediatype']) && (count($m['metadata']['mediatype']) > 0) && ($m['metadata']['mediatype'][0] == 'collection') )
          $is_collection = TRUE;
      else
          $is_collection = FALSE;
      $ccdict = ESIndexer::get_custom_collections( $id, $ln_hint, $is_collection );
      foreach ( $ccdict as $ccfieldname => $ccval ) {
          if ( !is_array($ccval) && isset($ccval) ) {
            $xml .= '<' . $ccfieldname . '>' . $ccval . "</" . $ccfieldname . ">\n";
          } elseif ( (is_array($ccval)) && (count($ccval)>0) ) {
             foreach ( $ccval as $ccvalval ) {
              $xml .= '<' . $ccfieldname . '>' . $ccvalval . "</" . $ccfieldname . ">\n";
             }
          }
      }
      
      $xml .= "</metadata>\n</results>\n";

//      echo $xml;
      
      $dom = new DOMDocument();
      $ret = $dom->loadXML($xml);
      if ($ret === FALSE)
        return "could not parse DOM";
      $dom->encoding='utf-8';
      $dom->substituteEntities=false;

    
      $results = $dom->documentElement;
      
    
      if (isset($m['reviews'])  &&  is_array($m['reviews'])  &&  count($m['reviews']))
        Reviews::reviews2dom($m['reviews'], $id, $dom, $results, true);


      $max_mtime = 0;
      if (isset($m['files'])  &&  is_array($m['files'])){
        $top = $results->appendChild($dom->createElement('files'));
        $formats=array();
        foreach ($m['files'] as $file => $ary){
          if (isset($ary['mtime']))
            $max_mtime = max($max_mtime, $ary['mtime']);
          
          if (isset($ary['format'])){
            if (is_array($ary['format']))
              $ary['format'] = $ary['format'][0];

            $formats[$ary['format']]=1;
          }
        }
        ksort($formats);
        foreach ($formats as $format => $ignored)
          XML::createNodeUTF8($dom, "format", $format, $top);
      }

      $lastmod = ($max_mtime ? date("YmdHi.s", min(time(), $max_mtime)) : false);

      $xml = self::xslt($id, $dom, $lastmod);
      if ($xml === FALSE)
        fatal("XSLT failed");
      
      if ($post) {
        self::post($xml);
      } else if ($return_doc) {
        return self::xmllines_to_searchdoc(explode(PHP_EOL, $xml)); // NOTE: only for ELASTIC
      } else {
        echo $xml;
      }
    }
    catch (Exception $e){
      error_log($e->getMessage());
      //if ($dom) error_log($dom->saveXML());
      return $e->getMessage();
    }
    $success = true;
    return "indexed";
  }



  // For any favorites list that this item belongs in,
  // inserts "synthetic" <collection> tags into the item's _meta.xml
  private static function add_collections_of_favorites_lists($id, &$meta){
    // Ask bookmarks table for list of users who have bookmarked (v1) or favorited (v2) this item.
    $itemnames = DB::qvals("SELECT itemname FROM users WHERE id IN (SELECT user_id FROM bookmarks WHERE ",array('identifier'=>$id),") AND locked!=1");

    //printr($meta['collection']);
    //printr($itemnames);
    
    if (!count($itemnames))
      return;
    if (!is_array($meta['collection']))
      $meta['collection'] = array($meta['collection']);
      
    // Convert the list to "fav-.." collections and insert <collection>fav-... tags!
    foreach ($itemnames as $itemname){
      $collection = 'fav-'.ltrim($itemname,'@'); // convert to favorites collection name
      $meta['collection'][] = $collection;
    }
    if (SE_PRINT_DONT_POST)    
      error_log("<collection>s now: ".join("\t",$meta['collection']));
  }

  
  /* SOLR EXPANSION of ID:
   *   Since we know item has a members.json file...
   *   get list of "collection:ID" identifiers (NOW)
   *   get list of identifiers in members.json (SHOULD BE)
   *   then return the difference of identifiers between the two lists
   *   which is what the *caller* should queue for updates.
   */
  private static function update_members($id, $members){

    unset($members[$id]); // we're already updating now!  ;-)

    if (SE_PRINT_DONT_POST)
      error_log("WANTED TO BE:  ".join("\t",array_keys($members)));


    // RARE exceptional case where we want to make sure we talk to the authoritative SE MASTER!
    $url = configGetValue('search-engine-url-indexer')."?wt=csv&fl=identifier&q=collection:{$id}&rows=".self::MAX_FAVORITES;
    $now = explode("\n",file_get_contents($url));
    if (SE_PRINT_DONT_POST)
      error_log("THERE NOW:  ".join("\t",$now));

    foreach ($now as $member){
      if ($member=='identifier') continue; // CSV header, chuck
      if (strncmp('fav-', $member, 4)===0) continue; //xxx  for now, dont allow favoriting other favorite lists to avoid flapping and circular dependencies and slow commit times in SOLR!
      if ($members[$member])
        unset($members[$member]); // its wanted *and* is there now
      else
        $members[$member]=0; // is NOT wanted (future) *and* is there now -- queue to remove
    }

    unset($members[$id]); // MAKE SURE we never add ourself!! 8-)

    // anything left in $members needs to be removed or added, so iterate over the items and update them!
    $updates = array_keys($members);
    if (SE_PRINT_DONT_POST)    
      error_log("\nNEED TO UPDATE THESE:  ".join("\t",$updates));
    return $updates;
  }

  


  private static function xmllines_to_searchdoc($lines) // NOTE: only for ELASTIC
  {
    $copyFields = array('updatedate'    => 'oai_updatedate',
                        'publicdate'    => 'oai_updatedate',
                        'addeddate'     => 'oai_updatedate',
                        'indexdate'     => 'oai_updatedate',
                        'reviewdate'    => 'oai_updatedate',
                        'producer'      => 'publisher',
                        'taper'         => 'publisher',
                        'productionco'  => 'publisher',
                        );
    $doc = array();
    foreach ($lines as $line){
      foreach (explode('<field name="', $line) as $piece){
        if (preg_match('/([^"]*)">(.*)<\/field>/s', $piece, $mat)){
          $key = $mat[1];
          $val = $mat[2];
          if (isset(Search::$DATES[$key]) && @substr($val, '-00-00') !== false)
            $val = str_replace('-00-00', '-01-01', $val);

          if (isset(Search::$DATES[$key]) && @substr($val, '-00T') !== false)
            $val = str_replace('-00T', '-01T', $val);

          $val = str_replace('&#xD;', "\n", $val);
          $val = str_replace('&#xA;', "\n", $val);
          $doc[$key][] = $val;
          if (isset($copyFields[$key]) && ($key2 = $copyFields[$key]))
            $doc[$key2][] = $val;
        }
      }
    }

    if (isset($doc['language'])){
      $lang = $doc['language'];
      if (is_array($lang))
        $lang = $lang[0];
      $languageSorter = self::language_coalesce($lang);
      if ($languageSorter != 'skip')
        $doc['languageSorter'] = $languageSorter;
    }

    return $doc;
  }


  // NOTE: may return 'skip' -- indicating caller should toss
  private static function language_coalesce($lang)
  {
    // XXX reorder me
    // XXX encode or catch some cases e.g. unknown just delete languagesorter - language code no is this (not norwegian) in practice, also none n/a qaa (is 'reserved for local use')
    // XXX what to do with handwritten?
    // XXX zun is mostly spam
    $lang_fixes =
      array(
            'no linguistic content' => 'skip',
            'no' => 'skip',
            'undetermined' => 'skip',
            'unknown' => 'skip',
            'none' => 'skip',
            'no speech' => 'skip',
            'no language' => 'skip',
            'qaa' => 'skip', // 'language code reserved for local use'
            '|d' => 'skip',

            'miscellaneous languages' => 'Multiple',
            // polyglot

            'jam' => 'Music',
            'ambient noise wall' => 'Music',

            'english-handwritten' => 'Handwritten English',
            'english handwritten' => 'Handwritten English',
            'hebrew-handwritten' => 'Handwritten Hebrew',
            'spanish-handwritten' => 'Handwritten Spanish',
            'french-handwritten' => 'Handwritten French',
            'german-handwritten' => 'Handwritten German',
            'yiddish-handwritten' => 'Handwritten Yiddish',
            'polish-handwritten' => 'Handwritten Polish',

            'und' => 'undetermined',
            'sq' => 'Albanian',
            'sqi' => 'Albanian',
            'ast' => 'Asturian',
            'eus' => 'Basque',
            'bos' => 'Bosnian',
            'bra' => 'Braj',
            /* 'Català' => 'Catalan', */
            'zho' => 'Chinese',
            'cmn' => 'Mandarin Chinese',
            'man' => 'Mandarin Chinese',
            'zh-tw' => 'Chinese',
            'zh-cn' => 'Chinese',
            'zh' => 'Chinese',
            'hrv' => 'Croatian',
            'cro' => 'Croatian',
            'cs' => 'Czech',
            'ces' => 'Czech',
            'dak' => 'Dakota',
            'da' => 'Danish',
            'ell' => 'Greek',
            'nld' => 'Dutch',
            'fle' => 'Dutch',
            'français' => 'French',
            'francais' => 'French',
            'de' => 'German',
            'de-formal' => 'German',
            'deu' => 'German',
            'deutsch' => 'German',
            'ger' => 'German',
            'ell' => 'Greek',
            'el' => 'Greek',
            'hu' => 'Hungarian',
            'egy' => 'Egyptian',
            'american' => 'English',
            'en' => 'English',
            'ng' => 'English',
            'en_us' => 'English',
            'en-us' => 'English',
            'en-gb' => 'English',
            'en-ca' => 'English',
            'emg' => 'English',
            'engilsh' => 'English',
            'english' => 'English',
            'us english' => 'English',
            'american english' => 'English',
            'eng;fre' => 'English and French',
            'eng-fre' => 'English and French',
            'engfre' => 'English and French',
            'english/french' => 'English and French',
            'finnish; english' => 'English and Finnish',
            'finnish, english' => 'English and Finnish',
            'english; finnish' => 'English and Finnish',
            'english, spanish' => 'English and Spanish',
            'english, polski' => 'English and Polish',
            'arabic, english' => 'Arabic and English',
            'hu' => 'Hungarian',
            'isl' => 'Icelandic',
            /* 'lt' => 'Latin', */ // is lithuanian, but archive items seem to be mislabled latin
            'fas' => 'Persian',
            'fa' => 'Persian',
            'panjabi' => 'Punjabi',
            'ron' => 'Romanian',
            'Русский' => 'Russian',
            'mwr' => 'Marwari',
            'scots' => 'Scottish',
            'slk' => 'Slovak',
            'sk' => 'Slovak',
            'srp' => 'Serbian',
            'es' => 'Spanish',
            'spain' => 'Spanish',
            'español' => 'Spanish',
            'espanol' => 'Spanish',
            'sv' => 'Swedish',
            'svenska' => 'Swedish',
            'fi' => 'Finnish',
            'fra' => 'French',
            'fr' => 'French',
            'language not encoded' => 'Unknown',
            'n/a' => 'Unknown',
            'mul' => 'Multilingual',
            'he' => 'Hebrew',
            'pt-br' => 'Portuguese',
            'português' => 'Portuguese',
            'portugues' => 'Portuguese',
            'port' => 'Portuguese',
            'unknown' => 'Unknown',
            'Український' => 'Ukranian',
            'uk' => 'Ukranian',
            'jap' => 'Japanese',
            'ko' => 'Korean',
            'kab' => 'Kabyle',
            'kas' => 'Kashmiri',
            'ksh' => 'Kölsch',
            'cym' => 'Welsh',
            'sme' => 'Saami',
            'ota' => 'Ottoman Turkish',
            'tr' => 'Turkish',
            'türkçe' => 'Turkish',
            'yid' => 'Yiddish',
            'ar' => 'Arabic',
            'عربي' => 'Arabic',
            'العربية' => 'Arabic',
            'عربية ::: arabic' => 'Arabic',
            'عربية ::: Arabic' => 'Arabic',
            'www.way2allah.com' => 'Arabic',
            'www.back4allah.com' => 'Arabic',
            'www.rabania.com' => 'Arabic',
            'arabic, english' => 'Arabic and English',
            'arabe' => 'Arabic',
            'arab' => 'Arabic',
            'arbc' => 'Arabic',
            'arbic' => 'Arabic',
            'arabic videos' => 'Arabic',
            'عربية مع ترجمة إنجليزية ::: Arabic with English subtitles' => 'Arabic with English subtitles',
            'أوردو ::: Urdu' => 'Urdu',
            'بشتو ::: Pashto' => 'Pashto'
            );

    $starts = array('<a href' => 'skip',
                    'English (Primarily)' => 'English');


    $lang = trim($lang);
    $lang = trim($lang, '.');
    $lang = Details::textsLanguage(null, $lang, true);
    if (isset($lang_fixes[strtolower($lang)]))
      $lang = $lang_fixes[strtolower($lang)];
    else
      $lang = ucfirst($lang);
    //error_log($lang);

    foreach ($starts as $start => $val){
      if (strpos($lang, $start, 0) === 0)
        $lang = $val;
    }

    return $lang;
  }


  private static function appender(&$dom, $key, $val, $prefix_forum=true)
  {
    // add a "newline" first...
    $text = $dom->createTextNode("\n");
    $dom->documentElement->appendChild($text);

    // now add the meatie
    $node = XML::createNodeUTF8($dom, 'field', $val, $dom->documentElement);
    $node->setAttribute('name', ($prefix_forum ? 'forum'.ucfirst($key) : $key));
    if (in_array($key, array('subject','poster')))
      self::appender($dom, "{$key}Sorter", strtolower($val));
  }

  // suggest caller passes in string "DELETE" for 3rd arg when want to delete...

  private static function fixdate($str)
  {
    $str=trim($str);
    $len=strlen($str);
    if ($len==0  ||  $len>22)
      return false;

    $monum = 0;
    foreach(array("january", "february", "march", "april",
                  "may", "june", "july", "august",
                  "september", "october", "november", "december") as $mo)
    {
      $lo = strtolower($str);
      $monum++;
      if (strpos($lo, $mo) === 0)
      {
        if (preg_match('/^' . $mo . ' (\d\d\d\d)$/', $lo, $m))
        {
          return "{$m[1]}-{$monum}-01";
        }
        if (preg_match('/^' . $mo . ' (\d+),? (\d\d\d\d)$/', $lo, $m))
        {
          return "{$m[2]}-{$monum}-{$m[1]}";
        }
      }
    }
    if ($len==10 && preg_match('/^(\d\d\d\d)\-(\d\d)\-(\d\d)$/', $str, $m))
    {
      if ($m[2] > 12) {
        return "{$m[1]}-{$m[3]}-{$m[2]}";
      }
      if ($m[3] > 31) {
        return "{$m[1]}-{$m[2]}-01T{$m[4]}:{$m[5]}:{$m[6]}Z";
      }
    }
    if ($len==19 && preg_match('/^(\d\d\d\d)\-(\d\d)\-(\d\d) (\d\d):(\d\d):(\d\d)$/', $str, $m))
    {
      if ($m[2] > 12) {
        return "{$m[1]}-{$m[3]}-{$m[2]}T{$m[4]}:{$m[5]}:{$m[6]}Z";
      }
    }
    if ($len==20 && preg_match('/^(\d\d\d\d)\-(\d\d)\-(\d\d)T(\d\d):(\d\d):(\d\d)Z$/', $str, $m))
    {
      if ($m[2] > 12) {
        return "{$m[1]}-{$m[3]}-{$m[2]}T{$m[4]}:{$m[5]}:{$m[6]}Z";
      }

      if ($m[3] > 31) {
        return "{$m[1]}-{$m[2]}-01T{$m[4]}:{$m[5]}:{$m[6]}Z";
      }
    }
        
        
    if ($len==19  &&  preg_match('/^(\d\d\d\d\-\d\d\-\d\d) (\d\d:\d\d:\d\d)$/', $str))
      return $str;  // perfect!  ideal!
    
    if ($len==14  &&  preg_match('/^(\d\d\d\d)(\d\d)(\d\d)(\d\d)(\d\d)(\d\d)$/',$str, $m))
      return "{$m[1]}-{$m[2]}-{$m[3]} {$m[4]}:{$m[5]}:{$m[6]}";

    if ($len==4  &&  preg_match('/^(\d\d\d\d)$/',$str))
      return "{$str}-01-01 00:00:00"; // eg:  /details/dictamendelacomi00guat
                  
    // "creative" crap like this tanks the entire post!  so find and nix them...
    //    <date>1844 - 1848</date>
    if (preg_match('/^\d{4} *\- *\d{4}$/', $str))
      return false;                 
                  
    if (preg_match('/^(\d\d\d\d\-\d\d\-\d\d)[ ]+(\d\d:\d\d)$/', $str, $m))
      return "{$m[1]} {$m[2]}:00";
    
    if (preg_match('/^(\d\d\d\d\-\d\d\-\d\d)[ ]+(\d\d)$/', $str, $m))
      return "{$m[1]} {$m[2]}:00:00";
    
    if (preg_match('/^\d\d\d\d\:\d\d\:\d\d$/', $str))
      return str_replace(':','-',$str);
             
    if (preg_match('/^(\d\d\d\d)[ :\-]+(\d\d)$/',$str, $mat)) // eg: yyyy-mm   /details/thoughtthingsstu01bald
      return "{$mat[1]}-{$mat[2]}-01 00:00:00";
    
    if (preg_match('/^\d\d\d\d[ ]*\-[ ]*\d\d[ ]*\-[ ]*\d\d$/',$str))
      return str_replace(' ','',$str);
    
    if (preg_match('/^(\d\d)[ ]+(\d\d)[ ]+(\d\d\d\d)$/',$str, $mat)) // dd mm yyyy
      return "{$mat[3]}-{$mat[2]}-{$mat[1]}";
    
    if (preg_match('/^(\d\d\d\d)[ ]+(\d\d)[ ]+(\d\d)$/',$str, $mat)) // yyyy mm dd
      return "{$mat[1]}-{$mat[2]}-{$mat[3]}";
    
    if (preg_match('/^(\d\d)[ ]*\-[ ]*(\d\d)[ ]*\-[ ]*(\d\d\d\d)$/',$str, $mat)) // dd mm yyyy
      return "{$mat[3]}-{$mat[2]}-{$mat[1]}";

    if ($len==10  &&  preg_match('/^\d{10}$/', $str)) // eg: "1282926076"
      return false;

    $ndigits = strlen(preg_replace("/[^\d]/","",$str));
    if ($ndigits < 6)
      return false;

    // remove fat-fingered duped non-digit chars like "....13::14...."
    $str2 = preg_replace(array('/[\-][\-]+/', '/::+/'), array('-',':'),$str);
    if ($str2 != $str)
      return self::fixdate($str2);

    $str2 = preg_replace('/^(\d\d\d\d\-\d\d\-\d\d) \- (\d\d:\d\d:\d\d)$/', '$1 $2', $str); //eg: "2011-11-03 - 20:00:00"
    if ($str2 != $str)
      return $str2;

    return false;
  }
  

  private static function post(&$xml) // posts to SOLR or ELASTIC
  {
    $xml = str_replace('<'.'?xml version="1.0"?'.'>','',$xml);
    $xml = str_replace('<'.'?xml version="1.0" encoding="utf-8"?'.'>','',$xml);
    $xml = str_replace('<'.'?xml version="1.0" encoding="UTF-8"?'.'>','',$xml);

    if (self::$post_num==0){
      //Util::profile_start();
      self::$post_xml = '<add commitWithin="600000">';
    }
    if (!self::$post_start_time)
      self::$post_start_time = time();
    
    self::$post_xml .= $xml;
    self::$post_num++;
    self::$post_total++;
    
    if (self::$post_num >= self::$post_group_size)
      self::post_flush();
  }
  
  
  private static function startswith($str, $substr)
  {
    return strpos($str, $substr) === 0;
  }


  public static function post_flush($post_errors=array())
  {
    if (!self::$post_num)
      return;

    self::$post_xml .= '</add>';

    $bulklines = array();
    $postlines = array();
    $current = -1;
    //printr(self::$post_xml);
    self::$post_xml = str_replace("<field name=\"", "\n<field name=\"", self::$post_xml);
    foreach (explode(PHP_EOL, self::$post_xml) as $line){
      if ($line == "</doc>"){
        $doc = self::xmllines_to_searchdoc($postlines);
        $postlines = array();
        $current = -1;

        // Add some explicit flags to support filter indexes
        $doc['indexflag'] = array();
        $noindex = isset($doc['noindex'])
          && $doc['noindex'] != false
          && $doc['noindex'] != 'false';


/* Remove these collections (currently 6,024 things) */
/* mediatype:collection AND collection:web AND access-restricted:true OR collection:focused_crawls */

/* Remove these items (currently 918,830 things) */
/* mediatype:"web" AND collection:(archiveit* OR endof* OR ait* OR NO404 OR alexa* OR amazon* OR focused_crawls OR customcrawlservices OR webwidecrawl OR web-group-internal OR webcollection OR 20thcenturyweb OR accelovation OR cuilcrawl OR inaweb OR internetmemoryfoundation OR mercator_convert) */

        $excluded = false;

        if (!$excluded && $doc['mediatype'][0] == 'other')
        {
          $excluded = true;
        }

        if (in_array($doc['identifier'][0],
                     array('buzzfeed.com', 'pubmed-PMC3539963'))) {
          print_r($doc);
        }

        if (!$excluded && $doc['mediatype'][0] == 'collection'
            && (@in_array('web', $doc['collection'])
                && isset($doc['access-restricted'])
                || @in_array('focused_crawls', $doc['collection'])))
        {
          $excluded = true;
        }

        /* print_r($doc); */
        if (!$excluded && $doc['mediatype'][0] == 'web') {
          foreach((array)$doc['collection'] as $col)
          {
            foreach(array('archiveit', 'endof', 'ait', 'NO404', 'alexa',
                          'amazon', 'focused_crawls',
                          'customcrawlservices', 'webwidecrawl',
                          'web-group-internal', 'webcollection',
                          '20thcenturyweb', 'accelovation', 'cuilcrawl',
                          'inaweb', 'internetmemoryfoundation',
                          'mercator_convert') as $nope)
            {
              if (self::startswith($col, $nope)) {
                $excluded = true;
                break 2;
              }
            }
          }
        }

        if ($excluded)
        {
          print($doc['identifier'][0] . " excluded\n");
          $doc['indexflag'][] = 'excluded';
        }

        if ($noindex || $excluded)
        {
          $doc['indexflag'][] = 'noindex';
        }
        else
        {
          $doc['indexflag'][] = 'index';
          $doc['indexflag'][] = 'nonoindex';
        }
        $music = false;
        if (isset($doc['collection'])
            && (in_array('what_cd', (array)$doc['collection'])
                || in_array('acdc', (array)$doc['collection'])))
        {
          $music = true;
          $doc['indexflag'][] = 'music';
        }
        if (isset($doc['forumPost']))
        {
          $doc['indexflag'][] = 'forumpost';
        }

        $id = $doc['identifier'][0];
        $doc_bulklines = self::get_es_bulklines_for_doc($id, $doc, self::$es_index, 'item', 3, false);
        $bulklines = array_merge($bulklines, $doc_bulklines);
      }
      else
      {
        if (strpos($line, '<field name=') !== false)
        {
          $current += 1;
          $postlines[$current] = '';
        }
        if ($current > -1)
        {
          $postlines[$current] .= $line;
        }
      }
    }

    $bulklines[] = "\n";

    if (SE_PRINT_DONT_POST) {
      error_log(implode("\n",$bulklines));
    }
    else {
      $ci = curl_init();
      curl_setopt($ci, CURLOPT_URL, 'http://es-lb:9200/_bulk');
      curl_setopt($ci, CURLOPT_PORT, 9200);
      curl_setopt($ci, CURLOPT_TIMEOUT, 200);
      curl_setopt($ci, CURLOPT_RETURNTRANSFER, 1);
      curl_setopt($ci, CURLOPT_FORBID_REUSE, 0);
      curl_setopt($ci, CURLOPT_CUSTOMREQUEST, 'POST');
      curl_setopt($ci, CURLOPT_POSTFIELDS, implode("\n", $bulklines) . "\n");
      $response = curl_exec($ci);
      
      $j = json_decode($response, 1);
//        file_put_contents( "/var/log/es_post_responses.log", "$response\n", FILE_APPEND | LOCK_EX ); 

      /* print_r($j); */
      if ($j['errors']) {
        //print_r($j);
        foreach($j['items'] as $item) {
          if (isset($item['index']['error'])) {
            $post_errors[$item['index']['_id']] = $item['index']['error'];
          }
          if (isset($item['update']['error'])) {
            $post_errors[$item['update']['_id']] = $item['update']['error'];
          }
        }
        foreach($post_errors as $k => $v)
           file_put_contents( "/var/log/esindexer_post_errors.log", "$k\t$v\t ERROR\n", FILE_APPEND | LOCK_EX); 
      }
    }
      
    $tmp = time() - self::$post_start_time;
    $avgPerSec = ($tmp==0 ? 0 : (self::$post_total + count(self::$ids_to_delete)) / $tmp);
    
    error_log("\nrunning ".Util::duration($tmp,1).", average ".round($avgPerSec,1)."/sec");
    //Util::profile_end();

    self::$post_num = 0;
  }

  
  private static function get_membership_matches($list) {
    $ci = curl_init();
    curl_setopt($ci, CURLOPT_URL,
                'http://es-lb:9200/' . self::$es_index . '/item/_search?q=listmemberships:'
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

    $matches_doc = json_decode($response, 1);
    $match_ids = array();
    foreach($matches_doc['hits']['hits'] as $hit) {
      $match_ids[] = $hit['_id'];
    }
    return $match_ids;
  }


  private static function get_es_bulklines_for_doc($id, $doc, $index, $type,
                                                   $retry=3, $delete=false)
  {
    $bulklines = array();
    
    if ($delete) {

      $bulklines[] = '{"delete" : {"_index" : "' . $index
        . '", "_type" : "item", "_id": "' . $id . '" } }';

    } else {

      // Index the actual item.
      $index_params = array('index'
                             => array('_index' => $index,
                                      '_type' => $type,
                                      '_id' => $id));
                                      
      $update_params = array('update'
                             => array('_index' => $index,
                                      '_type' => $type,
                                      '_retry_on_conflict' => $retry,
                                      '_id' => $id));
      // - If no old doc, just copy in new doc (upsert.)
      // - If old doc present, run script:
      //   -  copy any existing listmemberships into new doc.
      //   - Merge listmemberships into new doc collections
      //   - Replace doc with new doc.
// ES disallows inlined scripts in 1.3.8; moved to config/script/ on ES nodes (Ansible playbook copies /petabox/search/*.groovy)      
      $script_name = 'smart_append_listmemberships';        
//      $script_name = 'overwrite_favorites';
      $script_params = array('script' => $script_name, 
                             'lang' => 'groovy',
                             'params' => array('new_doc' => $doc), // script copy
                             'upsert' => $doc); // replace if empty
                             
       $update_json = json_encode($update_params,(defined('JSON_UNESCAPED_UNICODE')?JSON_UNESCAPED_UNICODE:0));
       $bulklines[] = $update_json;
       $script_json = json_encode($script_params,(defined('JSON_UNESCAPED_UNICODE')?JSON_UNESCAPED_UNICODE:0));
       $bulklines[] = $script_json;

//       file_put_contents( "/var/log/es_post_responses.log", "{$update_json}\n{$script_json}\n", FILE_APPEND | LOCK_EX ); 

    }

    return $bulklines;
    
  }

  public static function queue_delete($id)
  {
    self::$ids_to_delete[] = $id;
    return true;
  }


  public static function flush_deletes()
  {
    if (!count(self::$ids_to_delete))
      return;


    $json = '{';
    $first = true;
    $bulklines = array();
    foreach(self::$ids_to_delete as $id)
    {
      $success = false;
      $doc = self::id2se($id, false, 0, $success, true);
      $doc_bulklines = self::get_es_bulklines_for_doc($id, $doc, self::$es_index, 'item', 3, true);
      $bulklines = array_merge($bulklines, $doc_bulklines);
    }

    $ci = curl_init();
    curl_setopt($ci, CURLOPT_URL, 'http://es-lb:9200/_bulk');
    curl_setopt($ci, CURLOPT_PORT, 9200);
    curl_setopt($ci, CURLOPT_TIMEOUT, 200);
    curl_setopt($ci, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ci, CURLOPT_FORBID_REUSE, 0);
    curl_setopt($ci, CURLOPT_CUSTOMREQUEST, 'POST');
    curl_setopt($ci, CURLOPT_POSTFIELDS, implode("\n", $bulklines) . "\n");
    $response = curl_exec($ci);
    /* $j = json_decode($response, 1); */
    /* /\* print($response); *\/ */
    /* if ($j['errors']) { */
    /*   print_r($j); */
    /* } */
    /* // printr($response); */
          
      self::$ids_to_delete = array();
    }
  
  
  private static function xslt($identifier, &$dom, $lastmod)
  {
    $meta = $dom->documentElement->getElementsByTagName('metadata');
    if (!($meta->length))
      fatal("item has no metadata");
    
    // see if the "_meta.xml" portion of the item ONLY has the
    // minimal fields from a "create.php" creation of an item
    // and has never had its mediatype + collection set for it, etc...
    $onlyStubFields = true;
    foreach ($meta->item(0)->childNodes as $child)
    {
      if (!in_array($child->nodeName, array('#text', 'identifier', 'uploader', 'addeddate')))
      {
        $onlyStubFields = false;
        break;
      }
    }
    if ($onlyStubFields)
      fatal("item is not yet public");
        

    // this makes sure the "identifier" field is set for EVERY item,
    // even if it's not in its _meta.xml
    $node = XML::createNodeUTF8($dom, 'identifier', $identifier);
    

    
    // This will set an OAI updatedate -- so use time of item's last XML change
    if ($lastmod  &&
        preg_match('/^(\d\d\d\d)(\d\d)(\d\d)(\d\d)(\d\d)\.(\d\d)$/',$lastmod,$t))
    {
      $node = XML::createNodeUTF8(
        $dom, 'indexdate', "{$t[1]}-{$t[2]}-{$t[3]} {$t[4]}:{$t[5]}:{$t[6]}");
    }
    
    $root = "{$_SERVER['DOCUMENT_ROOT']}/../..";
    $xsl  = "$root/www/sf/includes/lc-names.xsl";
    $xsl2 = "$root/www/sf/includes/index.xsl";
    
    
    if (!($dom2 = XML::xslt(null, $xsl, $dom)))
      fatal("could not XSLT with $xsl");
    //$ret  = $dom2; echo $ret->saveXML(); exit;
    
    if (!($ret = XML::xslt(null, $xsl2, $dom2)))
      fatal("could not XSLT with $xsl2");
    $ret = $ret->saveXML();

    
    return $ret;
  }

  
  private static function forumIdentifier($id)
  {
    return "..$id..";
  }


  public static function metadata_api_to_solr_xml($id, $post=false, $task=array(), $group_size=DEFAULT_GROUP_SIZE, &$success=false, $return_doc=false)//transitional.  dies soon.  do NOT use use "id2se()" directly instead!
  {
    return self::id2se($id, $post, $group_size, $success, $return_doc);
  }
}


// helpful for getting identifier lists for building from scratch:
/*
// Record the max finished catalog_done task *BEFORE* starting.
// If you're *REALLY* starting from scratch, see se_indexer.php
// for how to setup a max finished in brand new queue...
sql-nt 'select max(finished) from catalog_done' >| maxfin;

// Get list of all item dirs across cluster
env CMD_TIMEOUT=0 CMD_DIR=/tmp/ifinder   ~tracey/scripts/cmd.sh  .  /home/tracey/scripts/item-file-fast-finder.php - -;
cat /tmp/ifinder/*out | cut -f4 -d/ | shuf >| ids;

// full reindex!
petabox/sw/bin/se_indexer.php  --ids=ids  --queue=1  --priority=3;
*/  

