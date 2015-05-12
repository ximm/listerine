cut -f 15 -d ' ' linedump-cleaned_items.json.procfail | sort -u | sed "s/\(.*\).\{2\}/\1/" > missing_collections ;
php ../scrape_collections.php > scraped_collections.raw ;
grep DARK scraped_collections.raw > scraped_collections.DARK ;
grep FAILED scraped_collections.raw > scraped_collections.FAILED ;
grep -v DARK scraped_collections.raw | grep -v FAILED > scraped_collections ;
cat scraped_collections hand_added.json > TO_INDEX
