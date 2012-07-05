// $ANTLR 3.3 Nov 30, 2010 12:45:30 /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g 2011-04-12 20:44:50

   package com.nearinfinity.blur.jdbc.grammar;
   import com.nearinfinity.blur.jdbc.QueryData;
   import java.util.Map;
   import java.util.HashMap;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class BlurParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "SELECT", "COLUMN_NAME", "NAME", "IDENT", "FROM", "EQUALS", "STRING_LITERAL", "BOOLEAN_CLAUSE", "DIGIT", "LETTER", "DOT", "UNDERSCORE", "INTEGER", "WS", "';'", "'as'", "','", "'where'"
    };
    public static final int EOF=-1;
    public static final int T__18=18;
    public static final int T__19=19;
    public static final int T__20=20;
    public static final int T__21=21;
    public static final int SELECT=4;
    public static final int COLUMN_NAME=5;
    public static final int NAME=6;
    public static final int IDENT=7;
    public static final int FROM=8;
    public static final int EQUALS=9;
    public static final int STRING_LITERAL=10;
    public static final int BOOLEAN_CLAUSE=11;
    public static final int DIGIT=12;
    public static final int LETTER=13;
    public static final int DOT=14;
    public static final int UNDERSCORE=15;
    public static final int INTEGER=16;
    public static final int WS=17;

    // delegates
    // delegators


        public BlurParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public BlurParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return BlurParser.tokenNames; }
    public String getGrammarFileName() { return "/Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g"; }


      private List<QueryData> columns = new ArrayList<QueryData>();
      private List<QueryData> tables = new ArrayList<QueryData>();




    // $ANTLR start "parse"
    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:24:1: parse returns [Map<String,List<QueryData>> columnsTables] : query ';' ;
    public final Map<String,List<QueryData>> parse() throws RecognitionException {
        Map<String,List<QueryData>> columnsTables = null;

        columnsTables = new HashMap<String,List<QueryData>>();
        try {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:26:6: ( query ';' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:26:8: query ';'
            {
            pushFollow(FOLLOW_query_in_parse64);
            query();

            state._fsp--;

            match(input,18,FOLLOW_18_in_parse65); 
            columnsTables.put("columns",columns);columnsTables.put("tables",tables);

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return columnsTables;
    }
    // $ANTLR end "parse"


    // $ANTLR start "query"
    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:29:1: query : select_statement from_statement where_statement ;
    public final void query() throws RecognitionException {
        try {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:30:5: ( select_statement from_statement where_statement )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:30:7: select_statement from_statement where_statement
            {
            pushFollow(FOLLOW_select_statement_in_query89);
            select_statement();

            state._fsp--;

            pushFollow(FOLLOW_from_statement_in_query91);
            from_statement();

            state._fsp--;

            pushFollow(FOLLOW_where_statement_in_query93);
            where_statement();

            state._fsp--;


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "query"


    // $ANTLR start "select_statement"
    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:33:1: select_statement : SELECT ( columns )+ ;
    public final void select_statement() throws RecognitionException {
        try {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:34:6: ( SELECT ( columns )+ )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:34:11: SELECT ( columns )+
            {
            match(input,SELECT,FOLLOW_SELECT_in_select_statement119); 
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:34:18: ( columns )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==COLUMN_NAME) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:34:18: columns
            	    {
            	    pushFollow(FOLLOW_columns_in_select_statement121);
            	    columns();

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "select_statement"


    // $ANTLR start "columns"
    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:37:1: columns : c= COLUMN_NAME ( 'as' a= ( NAME | IDENT ) )? ( ',' d= COLUMN_NAME ( 'as' b= ( NAME | IDENT ) )? )* ;
    public final void columns() throws RecognitionException {
        Token c=null;
        Token a=null;
        Token d=null;
        Token b=null;

        try {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:38:6: (c= COLUMN_NAME ( 'as' a= ( NAME | IDENT ) )? ( ',' d= COLUMN_NAME ( 'as' b= ( NAME | IDENT ) )? )* )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:38:11: c= COLUMN_NAME ( 'as' a= ( NAME | IDENT ) )? ( ',' d= COLUMN_NAME ( 'as' b= ( NAME | IDENT ) )? )*
            {
            c=(Token)match(input,COLUMN_NAME,FOLLOW_COLUMN_NAME_in_columns152); 
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:38:25: ( 'as' a= ( NAME | IDENT ) )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==19) ) {
                alt2=1;
            }
            switch (alt2) {
                case 1 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:38:26: 'as' a= ( NAME | IDENT )
                    {
                    match(input,19,FOLLOW_19_in_columns155); 
                    a=(Token)input.LT(1);
                    if ( (input.LA(1)>=NAME && input.LA(1)<=IDENT) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }


                    }
                    break;

            }

            columns.add(new QueryData((c!=null?c.getText():null),(a!=null?a.getText():null)));a=null;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:38:104: ( ',' d= COLUMN_NAME ( 'as' b= ( NAME | IDENT ) )? )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0==20) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:38:105: ',' d= COLUMN_NAME ( 'as' b= ( NAME | IDENT ) )?
            	    {
            	    match(input,20,FOLLOW_20_in_columns171); 
            	    d=(Token)match(input,COLUMN_NAME,FOLLOW_COLUMN_NAME_in_columns174); 
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:38:122: ( 'as' b= ( NAME | IDENT ) )?
            	    int alt3=2;
            	    int LA3_0 = input.LA(1);

            	    if ( (LA3_0==19) ) {
            	        alt3=1;
            	    }
            	    switch (alt3) {
            	        case 1 :
            	            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:38:123: 'as' b= ( NAME | IDENT )
            	            {
            	            match(input,19,FOLLOW_19_in_columns177); 
            	            b=(Token)input.LT(1);
            	            if ( (input.LA(1)>=NAME && input.LA(1)<=IDENT) ) {
            	                input.consume();
            	                state.errorRecovery=false;
            	            }
            	            else {
            	                MismatchedSetException mse = new MismatchedSetException(null,input);
            	                throw mse;
            	            }


            	            }
            	            break;

            	    }

            	    columns.add(new QueryData((d!=null?d.getText():null),(b!=null?b.getText():null)));b=null;

            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "columns"


    // $ANTLR start "from_statement"
    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:41:1: from_statement : FROM ( tables )+ ;
    public final void from_statement() throws RecognitionException {
        try {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:42:6: ( FROM ( tables )+ )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:42:8: FROM ( tables )+
            {
            match(input,FROM,FOLLOW_FROM_in_from_statement216); 
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:42:13: ( tables )+
            int cnt5=0;
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( (LA5_0==NAME) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:42:13: tables
            	    {
            	    pushFollow(FOLLOW_tables_in_from_statement218);
            	    tables();

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    if ( cnt5 >= 1 ) break loop5;
                        EarlyExitException eee =
                            new EarlyExitException(5, input);
                        throw eee;
                }
                cnt5++;
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "from_statement"


    // $ANTLR start "tables"
    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:45:1: tables : t= NAME ( 'as' a= NAME )? ( ',' v= NAME ( 'as' b= NAME )? )* ;
    public final void tables() throws RecognitionException {
        Token t=null;
        Token a=null;
        Token v=null;
        Token b=null;

        try {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:46:6: (t= NAME ( 'as' a= NAME )? ( ',' v= NAME ( 'as' b= NAME )? )* )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:46:11: t= NAME ( 'as' a= NAME )? ( ',' v= NAME ( 'as' b= NAME )? )*
            {
            t=(Token)match(input,NAME,FOLLOW_NAME_in_tables254); 
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:46:18: ( 'as' a= NAME )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==19) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:46:19: 'as' a= NAME
                    {
                    match(input,19,FOLLOW_19_in_tables257); 
                    a=(Token)match(input,NAME,FOLLOW_NAME_in_tables261); 

                    }
                    break;

            }

            tables.add(new QueryData((t!=null?t.getText():null),(a!=null?a.getText():null)));a=null;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:46:87: ( ',' v= NAME ( 'as' b= NAME )? )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==20) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:46:88: ',' v= NAME ( 'as' b= NAME )?
            	    {
            	    match(input,20,FOLLOW_20_in_tables268); 
            	    v=(Token)match(input,NAME,FOLLOW_NAME_in_tables272); 
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:46:99: ( 'as' b= NAME )?
            	    int alt7=2;
            	    int LA7_0 = input.LA(1);

            	    if ( (LA7_0==19) ) {
            	        alt7=1;
            	    }
            	    switch (alt7) {
            	        case 1 :
            	            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:46:100: 'as' b= NAME
            	            {
            	            match(input,19,FOLLOW_19_in_tables275); 
            	            b=(Token)match(input,NAME,FOLLOW_NAME_in_tables279); 

            	            }
            	            break;

            	    }

            	    tables.add(new QueryData((v!=null?v.getText():null),(b!=null?b.getText():null)));b=null;

            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "tables"


    // $ANTLR start "where_statement"
    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:49:1: where_statement : 'where' ( COLUMN_NAME | NAME | IDENT ) EQUALS STRING_LITERAL ( BOOLEAN_CLAUSE ( COLUMN_NAME | NAME | IDENT ) EQUALS STRING_LITERAL )* ;
    public final void where_statement() throws RecognitionException {
        try {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:50:6: ( 'where' ( COLUMN_NAME | NAME | IDENT ) EQUALS STRING_LITERAL ( BOOLEAN_CLAUSE ( COLUMN_NAME | NAME | IDENT ) EQUALS STRING_LITERAL )* )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:50:8: 'where' ( COLUMN_NAME | NAME | IDENT ) EQUALS STRING_LITERAL ( BOOLEAN_CLAUSE ( COLUMN_NAME | NAME | IDENT ) EQUALS STRING_LITERAL )*
            {
            match(input,21,FOLLOW_21_in_where_statement308); 
            if ( (input.LA(1)>=COLUMN_NAME && input.LA(1)<=IDENT) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            match(input,EQUALS,FOLLOW_EQUALS_in_where_statement322); 
            match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_where_statement324); 
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:50:67: ( BOOLEAN_CLAUSE ( COLUMN_NAME | NAME | IDENT ) EQUALS STRING_LITERAL )*
            loop9:
            do {
                int alt9=2;
                int LA9_0 = input.LA(1);

                if ( (LA9_0==BOOLEAN_CLAUSE) ) {
                    alt9=1;
                }


                switch (alt9) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:50:68: BOOLEAN_CLAUSE ( COLUMN_NAME | NAME | IDENT ) EQUALS STRING_LITERAL
            	    {
            	    match(input,BOOLEAN_CLAUSE,FOLLOW_BOOLEAN_CLAUSE_in_where_statement327); 
            	    if ( (input.LA(1)>=COLUMN_NAME && input.LA(1)<=IDENT) ) {
            	        input.consume();
            	        state.errorRecovery=false;
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        throw mse;
            	    }

            	    match(input,EQUALS,FOLLOW_EQUALS_in_where_statement341); 
            	    match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_where_statement343); 

            	    }
            	    break;

            	default :
            	    break loop9;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "where_statement"

    // Delegated rules


 

    public static final BitSet FOLLOW_query_in_parse64 = new BitSet(new long[]{0x0000000000040000L});
    public static final BitSet FOLLOW_18_in_parse65 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_select_statement_in_query89 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_from_statement_in_query91 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_where_statement_in_query93 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SELECT_in_select_statement119 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_columns_in_select_statement121 = new BitSet(new long[]{0x0000000000000022L});
    public static final BitSet FOLLOW_COLUMN_NAME_in_columns152 = new BitSet(new long[]{0x0000000000180002L});
    public static final BitSet FOLLOW_19_in_columns155 = new BitSet(new long[]{0x00000000000000C0L});
    public static final BitSet FOLLOW_set_in_columns159 = new BitSet(new long[]{0x0000000000100002L});
    public static final BitSet FOLLOW_20_in_columns171 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_COLUMN_NAME_in_columns174 = new BitSet(new long[]{0x0000000000180002L});
    public static final BitSet FOLLOW_19_in_columns177 = new BitSet(new long[]{0x00000000000000C0L});
    public static final BitSet FOLLOW_set_in_columns181 = new BitSet(new long[]{0x0000000000100002L});
    public static final BitSet FOLLOW_FROM_in_from_statement216 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_tables_in_from_statement218 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_NAME_in_tables254 = new BitSet(new long[]{0x0000000000180002L});
    public static final BitSet FOLLOW_19_in_tables257 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_NAME_in_tables261 = new BitSet(new long[]{0x0000000000100002L});
    public static final BitSet FOLLOW_20_in_tables268 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_NAME_in_tables272 = new BitSet(new long[]{0x0000000000180002L});
    public static final BitSet FOLLOW_19_in_tables275 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_NAME_in_tables279 = new BitSet(new long[]{0x0000000000100002L});
    public static final BitSet FOLLOW_21_in_where_statement308 = new BitSet(new long[]{0x00000000000000E0L});
    public static final BitSet FOLLOW_set_in_where_statement310 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUALS_in_where_statement322 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_where_statement324 = new BitSet(new long[]{0x0000000000000802L});
    public static final BitSet FOLLOW_BOOLEAN_CLAUSE_in_where_statement327 = new BitSet(new long[]{0x00000000000000E0L});
    public static final BitSet FOLLOW_set_in_where_statement329 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EQUALS_in_where_statement341 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_where_statement343 = new BitSet(new long[]{0x0000000000000802L});

}