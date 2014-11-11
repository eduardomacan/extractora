extractora
==========

Simple hack to recursively extract data from oracle tables. Useful for testcase generation. My QA team was
having a hard time extracting data samples from a complex and not enough documented oracle database in 
order to build new automated test cases, so I created this hack to help them. It worked :)

Install
----------

Rename dotextractoracfg-sample to ~/.dotextractoracfg and edit parameters there, it's a json file.

Configuration Options
----------

  * dsn: Oracle DSN
  * user: Oracle Database Username
  * password: Oracle Username Password
  * schema: Schema owner (usually the same as 'user', but not necessarily so)
  
Usage
----------

    usage: extractora.py [-h] [--xml] [--file OUTPUTFILE] table column value
    
    recursively extract data from oracle
    
    positional arguments:
      table                 table name
      column                column name
      value                 value name
    
    optional arguments:
      -h, --help            show this help message and exit
      --xml, -x             XML output (default=SQL)
      --file OUTPUTFILE, -f OUTPUTFILE
                            output filename (none for stdout)