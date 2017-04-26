package it.unimi.di.big.mg4j.mock.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.AlignDocumentIterator;
import it.unimi.di.big.mg4j.search.DifferenceDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.IntArrayDocumentIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
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

public class MockDocumentIteratorTest {
	private static Index index;
	private static SimpleParser simpleParser;
	private static String basename;

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		basename = File.createTempFile( OrDocumentIterator.class.getSimpleName(), "test" ).getCanonicalPath();
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
		
		assertEquals( END_OF_LIST, orDocumentIterator.nextDocument() );
	}
	
	@Test
	public void testLow() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 2, 3, 4, 5, 6, 7 }, 
				new int[][][] { 
				{ { 0, 10 } }, //0
				{ { 0, 10 }, { 10, 13 } }, //2
				{ { 0, 10 }, { 10, 14 } }, //3
				{ {} }, //4
				{ {} }, //5
				{}, //6
				{}, //7
				} );
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 4, 5, 6, 7 }, 
				new int[][][] { 
				{ { 10 } }, //1
				{ { 10, 12 }, { 11, 15 } }, //2
				{ {} }, //3
				{ {} }, //4
				{}, //5
				{ {} }, //6
				{}, //7
				} );
		DocumentIterator documentIterator = new LowPassDocumentIterator( OrDocumentIterator.getInstance( i0, i1 ), 5 );
		
		assertTrue( documentIterator.mayHaveNext() );
		assertTrue( documentIterator.mayHaveNext() ); // To increase coverage
		
		assertEquals( 1, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 10, 10 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 2, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 10, 12 ), documentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 11, 15 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 3, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 10, 14 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 4, documentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, documentIterator.intervalIterator() );
		
		assertEquals( 5, documentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, documentIterator.intervalIterator() );

		assertEquals( 6, documentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, documentIterator.intervalIterator() );
		
		assertEquals( END_OF_LIST, documentIterator.nextDocument() );
	}
	

	@Test
	public void testConsecutive() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 0, 10 } }, //0
				{ { 3 } }, //1
				{ { 0, 10 }, { 10, 13 } }, //2
				{ { 0, 10 }, { 10, 14 } }, //3
				{ {} }, //4
				} );
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 4 }, 
				new int[][][] { 
				{ { 10 } }, //1
				{ { 10, 12 }, { 11, 15 } }, //2
				{ {} }, //3
				{ {} }, //4
				} );
		DocumentIterator documentIterator = ConsecutiveDocumentIterator.getInstance( i0, i1 );
		
		assertTrue( documentIterator.mayHaveNext() );
		assertTrue( documentIterator.mayHaveNext() ); // To increase coverage
		
		assertEquals( 2, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 15 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 3, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 10 ), documentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 10, 14 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 4, documentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, documentIterator.intervalIterator() );
		
		assertFalse( documentIterator.mayHaveNext() );
	}
	
	@Test
	public void testAnd() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 2, 3, 4, 5, 6, 7 }, 
				new int[][][] { 
				{ { 0, 10 } }, //0
				{ { 0, 10 }, { 10, 13 } }, //2
				{ { 0, 10 }, { 10, 14 } }, //3
				{ {} }, //4
				{ {} }, //5
				{}, //6
				{}, //7
				} );
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 1, 2, 3, 4, 5, 6, 7 }, 
				new int[][][] { 
				{ { 10 } }, //1
				{ { 10, 12 }, { 11, 15 } }, //2
				{ {} }, //3
				{ {} }, //4
				{}, //5
				{ {} }, //6
				{}, //7
				} );
		DocumentIterator documentIterator = AndDocumentIterator.getInstance( i0, i1 );
		
		assertTrue( documentIterator.mayHaveNext() );
		assertTrue( documentIterator.mayHaveNext() ); // To increase coverage
		
		assertEquals( 2, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 12 ), documentIterator.intervalIterator().nextInterval() );
		assertTrue( documentIterator.intervalIterator() != IntervalIterators.FALSE );
		assertEquals( Interval.valueOf( 10, 13 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 3, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 10 ), documentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 10, 14 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 4, documentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, documentIterator.intervalIterator() );
		
		assertEquals( 5, documentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, documentIterator.intervalIterator() );

		assertEquals( 6, documentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, documentIterator.intervalIterator() );
		
		assertEquals( END_OF_LIST, documentIterator.nextDocument() );
	}

	@Test
	public void testConsecutiveWithGaps() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 10, 20 }, 
				new int[][][] { 
				{ { 104, 112 } }, //0
				{ { 3 } }, //1
				{ { 104, 112 } }, //2
				} );
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0, 10, 20 }, 
				new int[][][] { 
				{ { 123, 127 } }, //0
				{ { 3 } }, //1
				{ { 123, 127 } }, //2
				} );
		DocumentIterator i2 = new IntArrayDocumentIterator( new long[] { 0, 10, 20 }, 
				new int[][][] { 
				{ {} }, //0
				{ { 3 } }, //1
				{ { 138, 141 } }, //2
				} );
		DocumentIterator i3 = new IntArrayDocumentIterator( new long[] { 0, 10, 20 }, 
				new int[][][] { 
				{ { 148, 150 } }, //0
				{ { 3 } }, //1
				{ { 152 } }, //2
				} );
		DocumentIterator documentIterator = ConsecutiveDocumentIterator.getInstance( 
					new DocumentIterator[] { i0, i1, i2, i3 },
					new int[] { 104, 10, 10, 10 }
				);
		
		assertTrue( documentIterator.mayHaveNext() );
		assertTrue( documentIterator.mayHaveNext() ); // To increase coverage
		
		assertEquals( 0, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 150 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 20, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 0, 152 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertFalse( documentIterator.mayHaveNext() );
	}
	

	@Test
	public void testAlign() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 10, 20 }, 
				new int[][][] { 
				{ { 104, 112 }, { 105, 120 }, { 110, 127 } }, //0
				{ { 3 } }, //1
				{ { 104, 112 } }, //2
				} );
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0, 10, 20 }, 
				new int[][][] { 
				{ { 104, 112 }, { 105, 115 }, { 110, 127 } }, //0
				{ { 3 } }, //1
				{ { 123, 127 } }, //2
				} );
		DocumentIterator documentIterator = AlignDocumentIterator.getInstance( i0, i1 ); 
		
		assertTrue( documentIterator.mayHaveNext() );
		assertTrue( documentIterator.mayHaveNext() ); // To increase coverage
		
		assertEquals( 0, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 104, 112 ), documentIterator.intervalIterator().nextInterval() );
		assertTrue( documentIterator.intervalIterator() != IntervalIterators.FALSE );
		assertEquals( Interval.valueOf( 110, 127 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 10, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 3 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );
	}
	
	@Test
	public void testNot() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 10, 20 }, 
				new int[][][] { 
				{ { 104, 112 }, { 105, 120 }, { 110, 127 } }, //0
				{ { 3 } }, //1
				{ { 104, 112 } }, //2
				} );

		DocumentIterator documentIterator = NotDocumentIterator.getInstance( i0, 24 ); 
		assertTrue( documentIterator.mayHaveNext() );
		assertTrue( documentIterator.mayHaveNext() ); // To increase coverage
		
		for ( int i = 1; i < 10; i++ ) assertEquals( i, documentIterator.nextDocument() );
		for ( int i = 11; i < 20; i++ ) assertEquals( i, documentIterator.nextDocument() );
		for ( int i = 21; i < 24; i++ ) assertEquals( i, documentIterator.nextDocument() );
	}

	@Test
	public void testDifference() throws IOException {
		DocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 10, 20 }, 
				new int[][][] { 
				{ { 104, 112 }/*killed*/, { 105, 120 }/*not killed*/, { 110, 127 }/*killed*/ }, //0
				{ { 3 } }, //1
				{ { 104, 112 } }, //2
				} );
		DocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0, 10, 20 }, 
				new int[][][] { 
				{ { 106, 107 }, { 107, 119 }, { 120 } }, //0
				{ { 3 } }, //1
				{ { 106, 107 } }, //2
				} );
		DocumentIterator documentIterator = DifferenceDocumentIterator.getInstance( i0, i1, 2, 3 ); 
		
		assertTrue( documentIterator.mayHaveNext() );
		assertTrue( documentIterator.mayHaveNext() ); // To increase coverage
		
		assertEquals( 0, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 105, 120 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );

		assertEquals( 10, documentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 3 ), documentIterator.intervalIterator().nextInterval() );
		assertNull( documentIterator.intervalIterator().nextInterval() );
	}
}
