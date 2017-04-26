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
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OrDocumentIteratorTest {
	private static Index index;
	private static SimpleParser simpleParser;
	private static String basename;

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		basename = File.createTempFile( OrDocumentIteratorTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename, new StringArrayDocumentCollection( "a", "b", "c" ) ).run();
		index = Index.getInstance( basename + "-text", true, true );
		simpleParser = new SimpleParser( index.termProcessor );
	}

	@AfterClass
	public static void tearDown() {
		for( File f: new File( basename ).getParentFile().listFiles( (FileFilter)new PrefixFileFilter( new File( basename ).getName() ) ) )	f.delete();
	}

	@Test
	public void testSkipBug() throws QueryParserException, QueryBuilderVisitorException, IOException {
		Query query = simpleParser.parse( "a | b | c" );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, index, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		assertEquals( 2, documentIterator.skipTo( 2 ) );
		documentIterator.dispose();
	}
	
	@Test
	public void testOr() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 2, 3, 4, 5, 6, 7 }, 
				new int[][][] { 
				{ { 0, 1 } }, 
				{ { 0, 1 }, { 1, 2 } },
				{ { 0, 1 }, { 1, 2 } },
				{ {} },
				{ {} },
				{},
				{},
				} );
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 4, 5, 6, 7 }, 
				new int[][][] { 
				{ { 1 } },
				{ { 1, 3 }, { 3, 4 } },
				{ {} },
				{ {} },
				{},
				{ {} },
				{},
				} );
		OrDocumentIterator orDocumentIterator = (OrDocumentIterator)OrDocumentIterator.getInstance( i0, i1 );
		assertTrue( orDocumentIterator.mayHaveNext() );
		
		assertEquals( 0, orDocumentIterator.nextDocument() );
		assertTrue( orDocumentIterator.intervalIterator() != IntervalIterators.FALSE );
		assertTrue( orDocumentIterator.intervalIterator() != IntervalIterators.FALSE ); // To increase coverage
		assertEquals( Interval.valueOf( 0, 1 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() ); // To increase coverage
		
		assertEquals( 1, orDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 1, 1 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() );

		assertEquals( 2, orDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 3, 4 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() );

		assertEquals( 3, orDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 1 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() );

		assertEquals( 4, orDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, orDocumentIterator.intervalIterator() );
		
		assertEquals( 5, orDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, orDocumentIterator.intervalIterator() );

		assertEquals( 6, orDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, orDocumentIterator.intervalIterator() );
		
		assertEquals( 7, orDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.FALSE, orDocumentIterator.intervalIterator() );
		assertEquals( END_OF_LIST, orDocumentIterator.nextDocument() );
	}
	
	@Test
	public void testExtentDocumentIterator() throws IOException {
		IntArrayDocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 2, 3, 4 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 1, 5 } }, 
				{ {} },
				{ {} }
				} );
		IntArrayDocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 5, 7 } }, 
				{ { 2, 4 } }, 
				{ { 2, 4 } }, 
				{ {} }
				} );
		
		DocumentIterator orDocumentIterator = OrDocumentIterator.getInstance( i0, i1 );
		assertEquals( 0, orDocumentIterator.nextDocument() );
		assertEquals( 2, orDocumentIterator.intervalIterator().extent() );
		assertEquals( 1, orDocumentIterator.nextDocument() );
		assertEquals( 3, orDocumentIterator.intervalIterator().extent() );
		assertEquals( 2, orDocumentIterator.nextDocument() );
		assertEquals( 3, orDocumentIterator.intervalIterator().extent() );
		assertEquals( 3, orDocumentIterator.nextDocument() );
		assertEquals( 3, orDocumentIterator.intervalIterator().extent() );
		assertEquals( 4, orDocumentIterator.nextDocument() );
		assertEquals( END_OF_LIST, orDocumentIterator.nextDocument() );
	}

	@Test
	public void testIndexIterator() throws IOException {
		IntArrayIndexIterator i0 = new IntArrayIndexIterator( new long[] { 0, 2, 3, 4 }, 
				new int[][] { 
				{ 0, 1, 2 }, 
				{ 0, 3 }, 
				{ 3 },
				{ 5 }
				} );
		IntArrayIndexIterator i1 = new IntArrayIndexIterator( new long[] { 1, 2, 3, 4 }, 
				new int[][] {
				{ 1, 2 },
				{ 1 },
				{ 2, 3, 4 },
				{ 1, 2 }
				} );
		
		DocumentIterator orDocumentIterator = OrDocumentIterator.getInstance( i0, i1 );
		assertEquals( 0, orDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 1, orDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 1 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 2, orDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 3 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 3, orDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 2 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 3 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 4 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 4, orDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 1 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 2 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 5 ), orDocumentIterator.intervalIterator().nextInterval() );
		assertNull( orDocumentIterator.intervalIterator().nextInterval() );
	}
}
