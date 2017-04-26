package it.unimi.di.big.mg4j.index.snowball;

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.lang.MutableString;

import org.junit.Test;

public class PorterStemmerTest {

	@Test
	public void testShort() {
		PorterStemmer stemmer = new PorterStemmer();
		
		MutableString s = new MutableString();
		s.append( 's' );
		stemmer.processTerm( s );
		assertEquals( "s", s.toString() );

		s.append( 's' );
		stemmer.processTerm( s );
		assertEquals( "ss", s.toString() );

	
		s.length( 0 );

		s.append( 'S' );
		stemmer.processTerm( s );
		assertEquals( "s", s.toString() );

		s.append( 's' );
		stemmer.processTerm( s );
		assertEquals( "ss", s.toString() );

	}
}
