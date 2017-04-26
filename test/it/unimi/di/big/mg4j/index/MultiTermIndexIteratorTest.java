package it.unimi.di.big.mg4j.index;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.IntArrayIndexIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.di.big.mg4j.search.OrDocumentIterator;
import it.unimi.di.big.mg4j.search.visitor.AbstractDocumentIteratorVisitor;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.dsi.util.Interval;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiTermIndexIteratorTest {
	private static Index index;
	private static SimpleParser simpleParser;

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		String basename = File.createTempFile( MultiTermIndexIteratorTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename, new StringArrayDocumentCollection( "a", "b", "c" ) ).run();
		index = Index.getInstance( basename + "-text", true, true );
		simpleParser = new SimpleParser( index.termProcessor );
	}

	@Test
	public void testSkipBug() throws QueryParserException, QueryBuilderVisitorException, IOException {
		Query query = simpleParser.parse( "a + b + c" );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, index, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		assertEquals( 2, documentIterator.skipTo( 2 ) );
		documentIterator.dispose();
	}

	@Test
	public void test() throws IOException {
		IndexIterator i0 = new IntArrayIndexIterator( new long[] { 0, 1, 2 },
				new int[][] {
						{ 0, 3 },
						{ 0 },
						{ 0 },
				} );
		IndexIterator i1 = new IntArrayIndexIterator( new long[] { 0, 2 },
				new int[][] {
						{ 1 },
						{ 1 },
				} );
		IndexIterator i2 = new IntArrayIndexIterator( new long[] { 0, 1, 3 },
				new int[][] {
						{ 2 },
						{ 2 },
						{ 0 },
				} );
		MultiTermIndexIterator multiTermIndexIterator = (MultiTermIndexIterator)MultiTermIndexIterator.getInstance( i0, i1, i2 );
		assertEquals( 3, multiTermIndexIterator.frequency() );

		assertTrue( multiTermIndexIterator.mayHaveNext() );
		assertTrue( multiTermIndexIterator.mayHaveNext() ); // To increase coverage

		assertEquals( 0, multiTermIndexIterator.nextDocument() );
		assertTrue( multiTermIndexIterator.intervalIterator() != IntervalIterators.FALSE );
		assertTrue( multiTermIndexIterator.intervalIterator() != IntervalIterators.FALSE ); // To
																							// increase
																							// coverage
		assertEquals( Interval.valueOf( 0 ), multiTermIndexIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1 ), multiTermIndexIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2 ), multiTermIndexIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 3 ), multiTermIndexIterator.intervalIterator().nextInterval() );
		assertNull( multiTermIndexIterator.intervalIterator().nextInterval() );

		assertEquals( 1, multiTermIndexIterator.nextDocument() );
		assertTrue( multiTermIndexIterator.intervalIterator() != IntervalIterators.FALSE );
		assertTrue( multiTermIndexIterator.intervalIterator() != IntervalIterators.FALSE ); // To
																							// increase
																							// coverage
		assertEquals( Interval.valueOf( 0 ), multiTermIndexIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2 ), multiTermIndexIterator.intervalIterator().nextInterval() );

		assertEquals( 2, multiTermIndexIterator.count() );

		assertNull( multiTermIndexIterator.intervalIterator().nextInterval() );

		assertEquals( 2, multiTermIndexIterator.nextDocument() );
		assertTrue( multiTermIndexIterator.intervalIterator() != IntervalIterators.FALSE );
		assertTrue( multiTermIndexIterator.intervalIterator() != IntervalIterators.FALSE ); // To
																							// increase
																							// coverage
		assertEquals( Interval.valueOf( 0 ), multiTermIndexIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1 ), multiTermIndexIterator.intervalIterator().nextInterval() );

		assertEquals( 2, multiTermIndexIterator.count() );

		assertNull( multiTermIndexIterator.intervalIterator().nextInterval() );

		// Here we get the iterator of the underlying IndexIterator
		assertEquals( 3, multiTermIndexIterator.nextDocument() );
		assertTrue( multiTermIndexIterator.intervalIterator() != IntervalIterators.FALSE );
		assertEquals( Interval.valueOf( 0 ), multiTermIndexIterator.intervalIterator().nextInterval() );

		assertEquals( 1, multiTermIndexIterator.count() );

		assertNull( multiTermIndexIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, multiTermIndexIterator.nextDocument() );
	}

	// Contributed by Fabien Campagne
	@Test
	public void testMG4JMultiTermPositionIssue() throws IllegalAccessException, NoSuchMethodException, ConfigurationException, IOException, InvocationTargetException, InstantiationException,
			ClassNotFoundException, URISyntaxException {
		String basename = File.createTempFile( getClass().getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename, new StringArrayDocumentCollection(
				"A B C D E F F G G",
				"G A T H S K L J W L",
				"E S K D L J F K L S J D L S J D",
				"E B"
				) ).run();
		Index index = Index.getInstance( basename + "-text", true, true );

		// / String query = "A| B+C+G|W|S+J";
		DocumentIterator iterator = OrDocumentIterator.getInstance(
				index.documents( "A" ),
				MultiTermIndexIterator.getInstance(
						index.documents( "B" ),
						index.documents( "C" ),
						index.documents( "G" )
						),
				index.documents( "W" ),
				MultiTermIndexIterator.getInstance(
						index.documents( "S" ),
						index.documents( "J" )
						) );

		final long[] currDoc = new long[ 1 ];
		// A visitor invoking positionArray() on IndexIterators positioned on the current document.
		AbstractDocumentIteratorVisitor visitor = new AbstractDocumentIteratorVisitor() {
			public Boolean visit( IndexIterator indexIterator ) throws IOException {
				if ( indexIterator.count() > 0 && indexIterator.document() == currDoc[ 0 ] ) IndexIterators.positionArray( indexIterator );
				return Boolean.TRUE;
			}
		};

		for ( int document = 0; document < index.numberOfDocuments; document++ ) {
			currDoc[ 0 ] = iterator.skipTo( document );

			if ( document == currDoc[ 0 ] ) {
				iterator.accept( visitor ); // see method visit below.
			}
		}

		while ( currDoc[ 0 ] != END_OF_LIST ) {
			iterator.accept( visitor );
			currDoc[ 0 ] = iterator.nextDocument();
		}
	}
}
