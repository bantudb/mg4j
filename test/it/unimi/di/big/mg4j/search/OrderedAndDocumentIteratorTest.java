package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.dsi.util.Interval;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

public class OrderedAndDocumentIteratorTest {

	@Test
	public void testSkipBug() throws QueryParserException, QueryBuilderVisitorException, IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Index index;
		SimpleParser simpleParser;
		String basename = File.createTempFile( getClass().getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename, new StringArrayDocumentCollection( "a a b c d e" ) ).run();
		index = Index.getInstance( basename + "-text", true, true );
		simpleParser = new SimpleParser( index.termProcessor );

		Query query = simpleParser.parse( "(a|b|d)<(a|b|d)" );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, index, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		documentIterator.nextDocument();
		IntervalIterator intervalIterator = documentIterator.intervalIterator();
		assertEquals( Interval.valueOf( 0, 1 ), intervalIterator.nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), intervalIterator.nextInterval() );
		assertEquals( Interval.valueOf( 2, 4 ), intervalIterator.nextInterval() );
		assertNull( intervalIterator.nextInterval() );
		assertEquals( END_OF_LIST, documentIterator.nextDocument() );
		documentIterator.dispose();
	} 
	
	@Test
	public void testTrue() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } }, 
				{ {} }, 
				{ { 0 }, { 1 }, { 2 } }, 
				} );
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{ {} }, 
				{ { 2 } }, 
				{ { 0 }, { 1 }, { 2 } }, 
				} );
		DocumentIterator i2 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{ { 3 } }, 
				{ {} }, 
				{ { 0 }, { 1 }, { 2 } }, 
				} );
		DocumentIterator orderedAndDocumentIterator = OrderedAndDocumentIterator.getInstance( i0, i1, i2 );
		assertTrue( orderedAndDocumentIterator.mayHaveNext() );
		assertEquals( 0, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 0, orderedAndDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 1 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 1, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 1, orderedAndDocumentIterator.document() );
		assertEquals( Interval.valueOf( 1, 3 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 2, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 2, orderedAndDocumentIterator.document() );
		assertEquals( Interval.valueOf( 2 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 3, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 3, orderedAndDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, orderedAndDocumentIterator.nextDocument() );
	}

	@Test
	public void testAllAlignmentFailures() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 4 } }, 
				{ { 1 }, { 2 } }, 
				{ { 0, 1 }, { 1, 4 } }, 
				{ { 0, 1 }, { 1, 5 } }, 
				} );
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 2, 4 } }, 
				{ { 2, 4 }, { 5, 6 } }, 
				{ { 2 }, { 3 } }, 
				{ { 2, 3 }, { 5 } }, 
				{ { 2, 3 }, { 5 } }, 
				} );
		DocumentIterator i2 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 6, 7 } }, 
				{ { 6, 7 } }, 
				{ { 1, 2 }, { 2, 3 }, { 4 } }, 
				{ { 6, 7 } }, 
				{ { 6, 7 } }, 
				} );
		DocumentIterator orderedAndDocumentIterator = OrderedAndDocumentIterator.getInstance( i0, i1, i2 );
		assertTrue( orderedAndDocumentIterator.mayHaveNext() );
		assertEquals( 0, orderedAndDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 7 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 1, orderedAndDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 7 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 2, orderedAndDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 2, 4 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 3, orderedAndDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 1, 7 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 4, orderedAndDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 7 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, orderedAndDocumentIterator.nextDocument() );
	}
	
	@Test
	public void testLoopcounterPersistence() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 10, 20 }, { 30, 40 }, { 87, 88 } } 
		});
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 25, 27 }, { 45, 47 } } 
				} );
		DocumentIterator i2 = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 32, 40 }, { 35, 52 }, { 80, 90 }, { 120, 130 } }, 
				} );
		DocumentIterator i3 = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 49, 50 }, { 92, 105 }, { 140, 150 } }, 
				} );
		DocumentIterator orderedAndDocumentIterator = OrderedAndDocumentIterator.getInstance( i0, i1, i2, i3 );
		assertTrue( orderedAndDocumentIterator.mayHaveNext() );
		assertEquals( 0, orderedAndDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 10, 50 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 30, 105 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
	}

	@Test
	public void testLoopcounterPersistenceSingleton() throws IOException {
		DocumentIterator i0 = new IntArrayIndexIterator( new long[] { 0 }, 
				new int[][] { 
				{ 1, 4, 11 } 
		});
		DocumentIterator i1 = new IntArrayIndexIterator( new long[] { 0 }, 
				new int[][] { 
				{  2, 7 } 
				} );
		DocumentIterator i2 = new IntArrayIndexIterator( new long[] { 0 }, 
				new int[][] { 
				{ 7, 10, 14, 20 }, 
				} );
		DocumentIterator i3 = new IntArrayIndexIterator( new long[] { 0 }, 
				new int[][] { 
				{ 10, 15, 30 }, 
				} );
		DocumentIterator orderedAndDocumentIterator = OrderedAndDocumentIterator.getInstance( i0, i1, i2, i3 );
		assertTrue( orderedAndDocumentIterator.mayHaveNext() );
		assertEquals( 0, orderedAndDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 1, 10 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 4, 15 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, orderedAndDocumentIterator.nextDocument() );
	}

	@Test
	public void testIndexIntervalIterator() throws IOException {
		DocumentIterator i0 = new IntArrayIndexIterator( new long[] { 0, 1, 2, 3, 4, 5 }, 
				new int[][] { 
				{ 0, 1, 2 }, 
				{ 0 }, 
				{ 3 }, 
				{ 1, 2 }, 
				{ 1, 2 }, 
				{ 1, 3 },
				} );
		DocumentIterator i1 = new IntArrayIndexIterator( new long[] { 0, 1, 2, 3, 4, 5 }, 
				new int[][] { 
				{ 0, 1, 2 }, 
				{ 1 }, 
				{ 4 }, 
				{ 2, 4 }, 
				{ 3 }, 
				{ 2, 3 }
				} );
		DocumentIterator i2 = new IntArrayIndexIterator( new long[] { 0, 1, 2, 3, 4, 5 }, 
				new int[][] { 
				{ 0, 1, 2 }, 
				{ 2 }, 
				{ 2 }, 
				{ 4, 8 }, 
				{ 4 },
				{ 5 }
				} );
		DocumentIterator orderedAndDocumentIterator = OrderedAndDocumentIterator.getInstance( i0, i1, i2 );
		assertTrue( orderedAndDocumentIterator.mayHaveNext() );
		assertEquals( 0, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 0, orderedAndDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 1, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 1, orderedAndDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 3, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 3, orderedAndDocumentIterator.document() );
		assertEquals( Interval.valueOf( 1, 4 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2, 8 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 4, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 4, orderedAndDocumentIterator.document() );
		assertEquals( Interval.valueOf( 2, 4 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 5, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 5, orderedAndDocumentIterator.document() );
		assertEquals( Interval.valueOf( 1, 5 ), orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orderedAndDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, orderedAndDocumentIterator.nextDocument() );
	}

	@Test
	public void testExtentDocumentIterator() throws IOException {
		IntArrayDocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 1, 2 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 1 }, { 2 } }, 
				} );
		IntArrayDocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0, 1, 2 }, 
				new int[][][] { 
				{ { 5, 7 } }, 
				{ {} }, 
				{ { 2 }, { 3 }, { 4 } }, 
				} );
		
		DocumentIterator orderedAndDocumentIterator = OrderedAndDocumentIterator.getInstance( i0, i1 );
		assertEquals( 0, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 5, orderedAndDocumentIterator.intervalIterator().extent() );
		assertEquals( 1, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 2, orderedAndDocumentIterator.intervalIterator().extent() );
		assertEquals( 2, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 2, orderedAndDocumentIterator.intervalIterator().extent() );
		assertEquals( END_OF_LIST, orderedAndDocumentIterator.nextDocument() );
	}
	
	@Test
	public void testExtentIndexIterator() throws IOException {
		IntArrayIndexIterator i0 = new IntArrayIndexIterator( new long[] { 0 }, 
				new int[][] { 
				{ 0, 3 }, 
				} );
		IntArrayIndexIterator i1 = new IntArrayIndexIterator( new long[] { 0 }, 
				new int[][] { 
				{ 1, 4, 6 }, 
				} );

		DocumentIterator orderedAndDocumentIterator = OrderedAndDocumentIterator.getInstance( i0, i1  );
		assertEquals( 0, orderedAndDocumentIterator.nextDocument() );
		assertEquals( 2, orderedAndDocumentIterator.intervalIterator().extent() );
		assertEquals( END_OF_LIST, orderedAndDocumentIterator.nextDocument() );
	}
}
