// $ANTLR 3.3 Nov 30, 2010 12:45:30 /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g 2011-04-12 20:44:50

   package com.nearinfinity.blur.jdbc.grammar;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class BlurLexer extends Lexer {
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

    public BlurLexer() {;} 
    public BlurLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public BlurLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "/Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g"; }

    // $ANTLR start "T__18"
    public final void mT__18() throws RecognitionException {
        try {
            int _type = T__18;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:11:7: ( ';' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:11:9: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__18"

    // $ANTLR start "T__19"
    public final void mT__19() throws RecognitionException {
        try {
            int _type = T__19;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:12:7: ( 'as' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:12:9: 'as'
            {
            match("as"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__19"

    // $ANTLR start "T__20"
    public final void mT__20() throws RecognitionException {
        try {
            int _type = T__20;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:13:7: ( ',' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:13:9: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__20"

    // $ANTLR start "T__21"
    public final void mT__21() throws RecognitionException {
        try {
            int _type = T__21;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:14:7: ( 'where' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:14:9: 'where'
            {
            match("where"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__21"

    // $ANTLR start "DIGIT"
    public final void mDIGIT() throws RecognitionException {
        try {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:55:16: ( '0' .. '9' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:55:18: '0' .. '9'
            {
            matchRange('0','9'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "DIGIT"

    // $ANTLR start "LETTER"
    public final void mLETTER() throws RecognitionException {
        try {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:56:17: ( 'a' .. 'z' | 'A' .. 'Z' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:
            {
            if ( (input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "LETTER"

    // $ANTLR start "EQUALS"
    public final void mEQUALS() throws RecognitionException {
        try {
            int _type = EQUALS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:58:8: ( '=' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:58:10: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "EQUALS"

    // $ANTLR start "DOT"
    public final void mDOT() throws RecognitionException {
        try {
            int _type = DOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:59:5: ( '.' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:59:7: '.'
            {
            match('.'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DOT"

    // $ANTLR start "BOOLEAN_CLAUSE"
    public final void mBOOLEAN_CLAUSE() throws RecognitionException {
        try {
            int _type = BOOLEAN_CLAUSE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:60:16: ( 'AND' | 'and' | 'OR' | 'or' )
            int alt1=4;
            switch ( input.LA(1) ) {
            case 'A':
                {
                alt1=1;
                }
                break;
            case 'a':
                {
                alt1=2;
                }
                break;
            case 'O':
                {
                alt1=3;
                }
                break;
            case 'o':
                {
                alt1=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;
            }

            switch (alt1) {
                case 1 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:60:18: 'AND'
                    {
                    match("AND"); 


                    }
                    break;
                case 2 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:60:24: 'and'
                    {
                    match("and"); 


                    }
                    break;
                case 3 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:60:30: 'OR'
                    {
                    match("OR"); 


                    }
                    break;
                case 4 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:60:35: 'or'
                    {
                    match("or"); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BOOLEAN_CLAUSE"

    // $ANTLR start "SELECT"
    public final void mSELECT() throws RecognitionException {
        try {
            int _type = SELECT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:61:8: ( 'select' | 'SELECT' )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0=='s') ) {
                alt2=1;
            }
            else if ( (LA2_0=='S') ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:61:10: 'select'
                    {
                    match("select"); 


                    }
                    break;
                case 2 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:61:19: 'SELECT'
                    {
                    match("SELECT"); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "SELECT"

    // $ANTLR start "FROM"
    public final void mFROM() throws RecognitionException {
        try {
            int _type = FROM;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:62:6: ( 'from' | 'FROM' )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0=='f') ) {
                alt3=1;
            }
            else if ( (LA3_0=='F') ) {
                alt3=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:62:8: 'from'
                    {
                    match("from"); 


                    }
                    break;
                case 2 :
                    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:62:15: 'FROM'
                    {
                    match("FROM"); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "FROM"

    // $ANTLR start "UNDERSCORE"
    public final void mUNDERSCORE() throws RecognitionException {
        try {
            int _type = UNDERSCORE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:63:12: ( '_' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:63:14: '_'
            {
            match('_'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "UNDERSCORE"

    // $ANTLR start "COLUMN_NAME"
    public final void mCOLUMN_NAME() throws RecognitionException {
        try {
            int _type = COLUMN_NAME;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:64:13: ( IDENT DOT IDENT )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:64:15: IDENT DOT IDENT
            {
            mIDENT(); 
            mDOT(); 
            mIDENT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "COLUMN_NAME"

    // $ANTLR start "NAME"
    public final void mNAME() throws RecognitionException {
        try {
            int _type = NAME;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:65:6: ( ( LETTER )+ )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:65:8: ( LETTER )+
            {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:65:8: ( LETTER )+
            int cnt4=0;
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( ((LA4_0>='A' && LA4_0<='Z')||(LA4_0>='a' && LA4_0<='z')) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:65:8: LETTER
            	    {
            	    mLETTER(); 

            	    }
            	    break;

            	default :
            	    if ( cnt4 >= 1 ) break loop4;
                        EarlyExitException eee =
                            new EarlyExitException(4, input);
                        throw eee;
                }
                cnt4++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "NAME"

    // $ANTLR start "STRING_LITERAL"
    public final void mSTRING_LITERAL() throws RecognitionException {
        try {
            int _type = STRING_LITERAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:67:16: ( '\\'' ( . )* '\\'' )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:67:18: '\\'' ( . )* '\\''
            {
            match('\''); 
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:67:22: ( . )*
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( (LA5_0=='\'') ) {
                    alt5=2;
                }
                else if ( ((LA5_0>='\u0000' && LA5_0<='&')||(LA5_0>='(' && LA5_0<='\uFFFF')) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:67:22: .
            	    {
            	    matchAny(); 

            	    }
            	    break;

            	default :
            	    break loop5;
                }
            } while (true);

            match('\''); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "STRING_LITERAL"

    // $ANTLR start "INTEGER"
    public final void mINTEGER() throws RecognitionException {
        try {
            int _type = INTEGER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:68:9: ( ( DIGIT )+ )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:68:11: ( DIGIT )+
            {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:68:11: ( DIGIT )+
            int cnt6=0;
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( ((LA6_0>='0' && LA6_0<='9')) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:68:11: DIGIT
            	    {
            	    mDIGIT(); 

            	    }
            	    break;

            	default :
            	    if ( cnt6 >= 1 ) break loop6;
                        EarlyExitException eee =
                            new EarlyExitException(6, input);
                        throw eee;
                }
                cnt6++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "INTEGER"

    // $ANTLR start "IDENT"
    public final void mIDENT() throws RecognitionException {
        try {
            int _type = IDENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:69:7: ( LETTER ( LETTER | DIGIT )* )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:69:9: LETTER ( LETTER | DIGIT )*
            {
            mLETTER(); 
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:69:15: ( LETTER | DIGIT )*
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( ((LA7_0>='0' && LA7_0<='9')||(LA7_0>='A' && LA7_0<='Z')||(LA7_0>='a' && LA7_0<='z')) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:
            	    {
            	    if ( (input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop7;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "IDENT"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:70:4: ( ( ' ' | '\\t' | '\\n' | '\\r' | '\\f' )+ )
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:70:6: ( ' ' | '\\t' | '\\n' | '\\r' | '\\f' )+
            {
            // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:70:6: ( ' ' | '\\t' | '\\n' | '\\r' | '\\f' )+
            int cnt8=0;
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( ((LA8_0>='\t' && LA8_0<='\n')||(LA8_0>='\f' && LA8_0<='\r')||LA8_0==' ') ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:
            	    {
            	    if ( (input.LA(1)>='\t' && input.LA(1)<='\n')||(input.LA(1)>='\f' && input.LA(1)<='\r')||input.LA(1)==' ' ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    if ( cnt8 >= 1 ) break loop8;
                        EarlyExitException eee =
                            new EarlyExitException(8, input);
                        throw eee;
                }
                cnt8++;
            } while (true);

            _channel=HIDDEN;

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "WS"

    public void mTokens() throws RecognitionException {
        // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:8: ( T__18 | T__19 | T__20 | T__21 | EQUALS | DOT | BOOLEAN_CLAUSE | SELECT | FROM | UNDERSCORE | COLUMN_NAME | NAME | STRING_LITERAL | INTEGER | IDENT | WS )
        int alt9=16;
        alt9 = dfa9.predict(input);
        switch (alt9) {
            case 1 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:10: T__18
                {
                mT__18(); 

                }
                break;
            case 2 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:16: T__19
                {
                mT__19(); 

                }
                break;
            case 3 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:22: T__20
                {
                mT__20(); 

                }
                break;
            case 4 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:28: T__21
                {
                mT__21(); 

                }
                break;
            case 5 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:34: EQUALS
                {
                mEQUALS(); 

                }
                break;
            case 6 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:41: DOT
                {
                mDOT(); 

                }
                break;
            case 7 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:45: BOOLEAN_CLAUSE
                {
                mBOOLEAN_CLAUSE(); 

                }
                break;
            case 8 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:60: SELECT
                {
                mSELECT(); 

                }
                break;
            case 9 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:67: FROM
                {
                mFROM(); 

                }
                break;
            case 10 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:72: UNDERSCORE
                {
                mUNDERSCORE(); 

                }
                break;
            case 11 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:83: COLUMN_NAME
                {
                mCOLUMN_NAME(); 

                }
                break;
            case 12 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:95: NAME
                {
                mNAME(); 

                }
                break;
            case 13 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:100: STRING_LITERAL
                {
                mSTRING_LITERAL(); 

                }
                break;
            case 14 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:115: INTEGER
                {
                mINTEGER(); 

                }
                break;
            case 15 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:123: IDENT
                {
                mIDENT(); 

                }
                break;
            case 16 :
                // /Users/bbejeck/workspace/blur/blur-jdbc/src/main/java/com/nearinfinity/blur/jdbc/grammar/Blur.g:1:129: WS
                {
                mWS(); 

                }
                break;

        }

    }


    protected DFA9 dfa9 = new DFA9(this);
    static final String DFA9_eotS =
        "\2\uffff\1\25\1\uffff\1\25\2\uffff\7\25\1\uffff\1\25\3\uffff\1\41"+
        "\1\25\1\uffff\1\25\1\uffff\1\43\2\25\2\46\4\25\1\uffff\1\46\1\uffff"+
        "\1\25\1\46\1\uffff\7\25\2\63\1\64\2\25\2\uffff\2\67\1\uffff";
    static final String DFA9_eofS =
        "\70\uffff";
    static final String DFA9_minS =
        "\1\11\1\uffff\1\56\1\uffff\1\56\2\uffff\7\56\1\uffff\1\56\3\uffff"+
        "\2\56\1\uffff\1\56\1\uffff\11\56\1\uffff\1\56\1\uffff\2\56\1\uffff"+
        "\14\56\2\uffff\2\56\1\uffff";
    static final String DFA9_maxS =
        "\1\172\1\uffff\1\172\1\uffff\1\172\2\uffff\7\172\1\uffff\1\172\3"+
        "\uffff\2\172\1\uffff\1\172\1\uffff\11\172\1\uffff\1\172\1\uffff"+
        "\2\172\1\uffff\14\172\2\uffff\2\172\1\uffff";
    static final String DFA9_acceptS =
        "\1\uffff\1\1\1\uffff\1\3\1\uffff\1\5\1\6\7\uffff\1\12\1\uffff\1"+
        "\15\1\16\1\20\2\uffff\1\14\1\uffff\1\13\11\uffff\1\2\1\uffff\1\17"+
        "\2\uffff\1\7\14\uffff\1\11\1\4\2\uffff\1\10";
    static final String DFA9_specialS =
        "\70\uffff}>";
    static final String[] DFA9_transitionS = {
            "\2\22\1\uffff\2\22\22\uffff\1\22\6\uffff\1\20\4\uffff\1\3\1"+
            "\uffff\1\6\1\uffff\12\21\1\uffff\1\1\1\uffff\1\5\3\uffff\1\7"+
            "\4\17\1\15\10\17\1\10\3\17\1\13\7\17\4\uffff\1\16\1\uffff\1"+
            "\2\4\17\1\14\10\17\1\11\3\17\1\12\3\17\1\4\3\17",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\15\26\1\24\4\26\1"+
            "\23\7\26",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\7\26\1\31\22\26",
            "",
            "",
            "\1\27\1\uffff\12\30\7\uffff\15\26\1\32\14\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\21\26\1\33\10\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\21\26\1\34\10\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\4\26\1\35\25\26",
            "\1\27\1\uffff\12\30\7\uffff\4\26\1\36\25\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\21\26\1\37\10\26",
            "\1\27\1\uffff\12\30\7\uffff\21\26\1\40\10\26\6\uffff\32\26",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "",
            "",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\3\26\1\42\26\26",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\30\6\uffff\32\30",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\4\26\1\44\25\26",
            "\1\27\1\uffff\12\30\7\uffff\3\26\1\45\26\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\13\26\1\47\16\26",
            "\1\27\1\uffff\12\30\7\uffff\13\26\1\50\16\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\16\26\1\51\13\26",
            "\1\27\1\uffff\12\30\7\uffff\16\26\1\52\13\26\6\uffff\32\26",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\21\26\1\53\10\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\4\26\1\54\25\26",
            "\1\27\1\uffff\12\30\7\uffff\4\26\1\55\25\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\14\26\1\56\15\26",
            "\1\27\1\uffff\12\30\7\uffff\14\26\1\57\15\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\4\26\1\60\25\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\2\26\1\61\27\26",
            "\1\27\1\uffff\12\30\7\uffff\2\26\1\62\27\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\23\26\1\65\6\26",
            "\1\27\1\uffff\12\30\7\uffff\23\26\1\66\6\26\6\uffff\32\26",
            "",
            "",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            "\1\27\1\uffff\12\30\7\uffff\32\26\6\uffff\32\26",
            ""
    };

    static final short[] DFA9_eot = DFA.unpackEncodedString(DFA9_eotS);
    static final short[] DFA9_eof = DFA.unpackEncodedString(DFA9_eofS);
    static final char[] DFA9_min = DFA.unpackEncodedStringToUnsignedChars(DFA9_minS);
    static final char[] DFA9_max = DFA.unpackEncodedStringToUnsignedChars(DFA9_maxS);
    static final short[] DFA9_accept = DFA.unpackEncodedString(DFA9_acceptS);
    static final short[] DFA9_special = DFA.unpackEncodedString(DFA9_specialS);
    static final short[][] DFA9_transition;

    static {
        int numStates = DFA9_transitionS.length;
        DFA9_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA9_transition[i] = DFA.unpackEncodedString(DFA9_transitionS[i]);
        }
    }

    class DFA9 extends DFA {

        public DFA9(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 9;
            this.eot = DFA9_eot;
            this.eof = DFA9_eof;
            this.min = DFA9_min;
            this.max = DFA9_max;
            this.accept = DFA9_accept;
            this.special = DFA9_special;
            this.transition = DFA9_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__18 | T__19 | T__20 | T__21 | EQUALS | DOT | BOOLEAN_CLAUSE | SELECT | FROM | UNDERSCORE | COLUMN_NAME | NAME | STRING_LITERAL | INTEGER | IDENT | WS );";
        }
    }
 

}