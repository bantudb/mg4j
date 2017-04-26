package it.unimi.di.big.mg4j.query.nodes;

import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;

import org.junit.Test;


public class AbstractTermExpanderTest {

	@Test
	public void testExpand() throws QueryParserException {
		QueryTransformer addAqueryTransformer = new AbstractTermExpander() {
			@Override
			public Query expand( Term term ) {
				return new Term( "a" + term.term );
			}

			@Override
			public Query expand( Prefix prefix ) {
				return prefix;
			}
		};
		
		Query query = new SimpleParser().parse( "foo AND bar" );
		assertEquals( new And( new Term( "afoo"), new Term( "abar") ), addAqueryTransformer.transform( query ) ); 

		QueryTransformer expandQueryTransformer = new AbstractTermExpander() {
			/** The visitor used by this expander. */
			@Override
			public Query expand( Term term ) {
				return new MultiTerm( term, new Term( "a" + term.term ) );
			}

			@Override
			public Query expand( Prefix prefix ) {
				return prefix;
			}
		};
		
		assertEquals( new And( new MultiTerm( new Term( "foo" ), new Term( "afoo" ) ), new MultiTerm( new Term( "bar" ), new Term( "abar") ) ), expandQueryTransformer.transform( query ) ); 
	}
}
