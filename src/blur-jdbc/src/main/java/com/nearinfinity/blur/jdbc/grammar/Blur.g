grammar Blur;

options {
  language = Java;
}

@header {
   package com.nearinfinity.blur.jdbc.grammar;
   import com.nearinfinity.blur.jdbc.QueryData;
   import java.util.Map;
   import java.util.HashMap;
}
@members {
  private List<QueryData> columns = new ArrayList<QueryData>();
  private List<QueryData> tables = new ArrayList<QueryData>();

}
@lexer::header {
   package com.nearinfinity.blur.jdbc.grammar;
}
 


parse returns[Map<String,List<QueryData>> columnsTables]
     @init{$columnsTables = new HashMap<String,List<QueryData>>();} 
     : query';'{$columnsTables.put("columns",columns);$columnsTables.put("tables",tables);}
     ;
     
query
    : select_statement from_statement where_statement
    ;
     
select_statement
     :    SELECT columns+
     ;
     
columns 
     :    c=COLUMN_NAME ('as' a=(NAME | IDENT))? {columns.add(new QueryData($c.text,$a.text));$a=null;}(','d=COLUMN_NAME ('as' b=(NAME | IDENT))?{columns.add(new QueryData($d.text,$b.text));$b=null;})*
     ;
     
from_statement
     : FROM tables+
     ;     
     
tables 
     :    t=NAME ('as' a=NAME)? {tables.add(new QueryData($t.text,$a.text));$a=null;} (',' v=NAME ('as' b=NAME)?{tables.add(new QueryData($v.text,$b.text));$b=null;})*
     ;
     
where_statement
     : 'where' (COLUMN_NAME | NAME | IDENT) EQUALS STRING_LITERAL (BOOLEAN_CLAUSE (COLUMN_NAME | NAME | IDENT) EQUALS STRING_LITERAL)*
     ;
     


fragment DIGIT : '0'..'9';
fragment LETTER : 'a'..'z'|'A'..'Z' ;

EQUALS : '=';
DOT : '.';
BOOLEAN_CLAUSE : 'AND'|'and'|'OR'|'or';
SELECT : 'select'|'SELECT';
FROM : 'from'|'FROM';
UNDERSCORE : '_';
COLUMN_NAME : IDENT DOT IDENT;
NAME : LETTER+;

STRING_LITERAL : '\''.*'\'';
INTEGER : DIGIT+ ;
IDENT : LETTER(LETTER | DIGIT)* ;
WS : (' ' | '\t' | '\n' | '\r' | '\f')+  {$channel=HIDDEN;};