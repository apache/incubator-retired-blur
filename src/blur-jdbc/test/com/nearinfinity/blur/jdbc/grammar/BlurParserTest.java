package com.nearinfinity.blur.jdbc.grammar;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.TokenStream;
import org.junit.Test;

import com.nearinfinity.blur.jdbc.QueryData;

@SuppressWarnings("serial")
public class BlurParserTest {
	
	private BlurLexer blurLexer;
	private BlurParser blurParser;
	private CharStream charStream;
	private TokenStream tokenStream;
	
	@Test
	public void testGetColumnsNoAlias() throws Exception {
		String query = "select person.pn, person.ln, cert.br, cert.ba from person,certification where person.pn = 'smith';";
		setUpParsing(query);
		List<QueryData> expectedColumns = new ArrayList<QueryData>(){{
			add(new QueryData("person.pn",null));
			add(new QueryData("person.ln",null));
			add(new QueryData("cert.br",null));
			add(new QueryData("cert.ba",null));
		}};
		Map<String,List<QueryData>>columnsTables = blurParser.parse();
		assertThat(columnsTables.get("columns"),is(expectedColumns));
	}
	
	@Test
	public void testGetColumnsAlias() throws Exception {
		String query = "select person.pn as firstName, person.ln as lastName, cert.br as certNumber, cert.ba as certBar from person,certification where lastName = 'smith';";
		setUpParsing(query);
		List<QueryData> expectedColumns = new ArrayList<QueryData>(){{
			add(new QueryData("person.pn","firstName"));
			add(new QueryData("person.ln","lastName"));
			add(new QueryData("cert.br","certNumber"));
			add(new QueryData("cert.ba","certBar"));
		}};
		Map<String,List<QueryData>>columnsTables = blurParser.parse();
		assertThat(columnsTables.get("columns"),is(expectedColumns));
	}
	
	@Test
	public void testGetColumnsWithAndWithoutAlias() throws Exception {
		String query = "select person.pn as firstName, person.ln as lastName, cert.br, cert.ba from person,certification where lastName = 'smith';";
		setUpParsing(query);
		List<QueryData> expectedColumns = new ArrayList<QueryData>(){{
			add(new QueryData("person.pn","firstName"));
			add(new QueryData("person.ln","lastName"));
			add(new QueryData("cert.br",null));
			add(new QueryData("cert.ba",null));
		}};
		Map<String,List<QueryData>>columnsTables = blurParser.parse();
		assertThat(columnsTables.get("columns"),is(expectedColumns));
	}
	
	@Test
	public void testGetTableNamesNoAlias() throws Exception {
		String query = "select person.pn as firstName, person.ln as lastName, cert.br, cert.ba from person,certification where lastName = 'smith';";
		setUpParsing(query);
		List<QueryData> expectedTables = new ArrayList<QueryData>(){{
			add(new QueryData("person",null));
			add(new QueryData("certification",null));
		}};
		Map<String,List<QueryData>>columnsTables = blurParser.parse();
		assertThat(columnsTables.get("tables"),is(expectedTables));
	}
	
	@Test
	public void testGetColumnsTablesWithAlias() throws Exception {
		String query = "select person.pn as firstName, person.ln as lastName, cert.br as certNumber, cert.ba as certBar from person as per,certification as cert where lastName = 'smith';";
		setUpParsing(query);
		List<QueryData> expectedColumns = new ArrayList<QueryData>(){{
			add(new QueryData("person.pn","firstName"));
			add(new QueryData("person.ln","lastName"));
			add(new QueryData("cert.br","certNumber"));
			add(new QueryData("cert.ba","certBar"));
		}};
		
		List<QueryData> expectedTables = new ArrayList<QueryData>(){{
			add(new QueryData("person","per"));
			add(new QueryData("certification","cert"));
		}};
		Map<String,List<QueryData>>columnsTables = blurParser.parse();
		assertThat(columnsTables.get("tables"),is(expectedTables));
		assertThat(columnsTables.get("columns"),is(expectedColumns));
	}
	
	@Test
	public void testGetColumnsTablesWithIdentAlias() throws Exception {
		String query = "select person.pn as col1, person.ln as col2, cert.br as certNumber, cert.ba as certBar from person as per,certification as cert where col1 = 'smith';";
		setUpParsing(query);
		List<QueryData> expectedColumns = new ArrayList<QueryData>(){{
			add(new QueryData("person.pn","col1"));
			add(new QueryData("person.ln","col2"));
			add(new QueryData("cert.br","certNumber"));
			add(new QueryData("cert.ba","certBar"));
		}};
		
		List<QueryData> expectedTables = new ArrayList<QueryData>(){{
			add(new QueryData("person","per"));
			add(new QueryData("certification","cert"));
		}};
		Map<String,List<QueryData>>columnsTables = blurParser.parse();
		assertThat(columnsTables.get("tables"),is(expectedTables));
		assertThat(columnsTables.get("columns"),is(expectedColumns));
	}
	
	@Test
	public void testGetColumnsTablesWithAndWithoutAlias() throws Exception {
		String query = "select person.pn as firstName, person.ln as lastName, cert.br, cert.ba from person,certification as cert where lastName = 'smith';";
		setUpParsing(query);
		List<QueryData> expectedColumns = new ArrayList<QueryData>(){{
			add(new QueryData("person.pn","firstName"));
			add(new QueryData("person.ln","lastName"));
			add(new QueryData("cert.br",null));
			add(new QueryData("cert.ba",null));
		}};
		
		List<QueryData> expectedTables = new ArrayList<QueryData>(){{
			add(new QueryData("person",null));
			add(new QueryData("certification","cert"));
		}};
		Map<String,List<QueryData>>columnsTables = blurParser.parse();
		assertThat(columnsTables.get("columns"),is(expectedColumns));
		assertThat(columnsTables.get("tables"),is(expectedTables));
	}
	
	private void setUpParsing(String query){
		charStream = new ANTLRStringStream(query);
		blurLexer = new BlurLexer(charStream);
		tokenStream = new CommonTokenStream(blurLexer);
		blurParser = new BlurParser(tokenStream);
	}


}
