package it.unimi.di.big.mg4j.mock.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import it.unimi.di.big.mg4j.document.CompositeDocumentSequence;
import it.unimi.di.big.mg4j.document.DateArrayDocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.Align;
import it.unimi.di.big.mg4j.query.nodes.And;
import it.unimi.di.big.mg4j.query.nodes.Consecutive;
import it.unimi.di.big.mg4j.query.nodes.Containment;
import it.unimi.di.big.mg4j.query.nodes.Difference;
import it.unimi.di.big.mg4j.query.nodes.Inclusion;
import it.unimi.di.big.mg4j.query.nodes.LowPass;
import it.unimi.di.big.mg4j.query.nodes.MultiTerm;
import it.unimi.di.big.mg4j.query.nodes.Not;
import it.unimi.di.big.mg4j.query.nodes.Or;
import it.unimi.di.big.mg4j.query.nodes.OrderedAnd;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.nodes.Range;
import it.unimi.di.big.mg4j.query.nodes.Select;
import it.unimi.di.big.mg4j.query.nodes.Term;
import it.unimi.di.big.mg4j.query.nodes.True;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.di.big.mg4j.search.score.ScorerTest;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceArrayMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.util.Interval;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MockNonmockTest {
	/** The temporary directory where all tests are run. */
	private static File tempDir;

	private static final int nDocuments = 100;
	private static final int nIndices = 3;
	private static final int nSpecialIndices = 1;
	private static final int nNonposIndices = 1;
	private static final int maxDocLength = 100;
	private static final String[] dictionary = new String[] {  "a", "b", "c" };
	private static final int maxSubqueries = 5;
	private static final int maxMargin = 3;
	private static final int minLow = 3;
	private static final int maxLow = 10;
	private static final int maxGap = 10;

	private static StringArrayDocumentCollection[] documentCollection = new StringArrayDocumentCollection[ nIndices ];
	private static DateArrayDocumentCollection[] specialCollection = new DateArrayDocumentCollection[ nSpecialIndices ];
	private static StringArrayDocumentCollection[] nonposCollection = new StringArrayDocumentCollection[ nNonposIndices ];
	private static String[] basename = new String[ nIndices ];
	private static String[] specialBasename = new String[ nSpecialIndices ];
	private static String[] nonposBasename = new String[ nNonposIndices ];
	private static Object2ReferenceMap<String,Index> indexMap = new Object2ReferenceArrayMap<String, Index>();
	private static Object2ReferenceMap<Index,String> indexName = new Object2ReferenceArrayMap<Index,String>();
	private static Index[] index = new Index[ nIndices + nSpecialIndices + nNonposIndices ];
	private static Random random = new Random( 0 );

	
	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		// Create a new directory under /tmp
		tempDir = File.createTempFile( "mg4jtest", null );
		tempDir.delete();
		tempDir.mkdir();

		for ( int i = 0; i < nIndices; i++ ) {
			basename[ i ] = File.createTempFile( ScorerTest.class.getSimpleName(), "test" + i, tempDir ).toString();
			
			String[] docs = new String[ nDocuments ];
			for ( int d = 0; d < nDocuments; d++ ) {
				int docLength = random.nextInt( maxDocLength );
				StringBuilder sb = new StringBuilder();
				for ( int j = 0; j < docLength; j++ ) 
					sb.append( dictionary[ random.nextInt( dictionary.length ) ] + " " );
				docs[ d ] = sb.toString();
			}
			documentCollection[ i ] = new StringArrayDocumentCollection( docs ); 
			new IndexBuilder( basename[ i ], new CompositeDocumentSequence( new DocumentSequence[] { new StringArrayDocumentCollection( docs ) }, new String[] { "text" + i } ) ).run();
			index[ i ] = Index.getInstance( basename[ i ] + "-text" + i + "?mapped=1", true, true );
			indexMap.put( "index" + i, index[ i ] );
			indexName.put( index[ i ], "index" + i );
		}
		for ( int i = 0; i < nSpecialIndices; i++ ) {
			specialBasename[ i ] = File.createTempFile( ScorerTest.class.getSimpleName(), "special-test" + i, tempDir ).toString();
			
			Date[] docs = new Date[ nDocuments ];
			for ( int d = 0; d < nDocuments; d++ ) docs[ d ] = new Date( random.nextLong() % 1000000000L );
			specialCollection[ i ] = new DateArrayDocumentCollection( docs ); 
			new IndexBuilder( specialBasename[ i ], specialCollection[ i ] ).run();
			index[ nIndices + i ] = Index.getInstance( specialBasename[ i ] + "-date", true, true );
			indexMap.put( "special" + i, index[ nIndices + i ] );
			indexName.put( index[ nIndices + i ], "special" + i );
		}
		for ( int i = 0; i < nNonposIndices; i++ ) {
			Map<Component, Coding> writerFlags = new Object2ObjectOpenHashMap<CompressionFlags.Component, CompressionFlags.Coding>( CompressionFlags.DEFAULT_STANDARD_INDEX );
			writerFlags.remove( Component.POSITIONS );
			writerFlags.remove( Component.COUNTS );
			nonposBasename[ i ] = File.createTempFile( ScorerTest.class.getSimpleName(), "nonpos-test" + i, tempDir ).toString();
			
			String[] docs = new String[ nDocuments ];
			for ( int d = 0; d < nDocuments; d++ ) {
				int docLength = random.nextInt( maxDocLength );
				StringBuilder sb = new StringBuilder();
				for ( int j = 0; j < docLength; j++ ) 
					sb.append( dictionary[ random.nextInt( dictionary.length ) ] + " " );
				docs[ d ] = sb.toString();
			}
			nonposCollection[ i ] = new StringArrayDocumentCollection( docs );
			new IndexBuilder( nonposBasename[ i ], nonposCollection[ i ] ).standardWriterFlags( writerFlags ).run();
			index[ nIndices + nSpecialIndices + i ] = Index.getInstance( nonposBasename[ i ] + "-text", true, true );
			indexMap.put( "nonposindex" + i, index[ nIndices + nSpecialIndices + i ] );
			indexName.put( index[ nIndices + nSpecialIndices + i ], "nonposindex" + i );
		}
	}

	@AfterClass
	public static void tearDown() throws IOException {
		FileUtils.forceDelete( tempDir );
	}
	
	public LongSortedSet assertSame( DocumentIterator it0, DocumentIterator it1 ) throws IOException {
		LongSortedSet documents = assertSame( it0, it1, Integer.MAX_VALUE );
		it0.dispose();
		it1.dispose();
		return documents;
	}

	public LongSortedSet assertSame( DocumentIterator it0, DocumentIterator it1, int maxIter ) throws IOException {
		final LongRBTreeSet documents = new LongRBTreeSet();
		long d0 = -1, d1 = -1;
		while ( maxIter-- != 0 ) {
			d0 = it0.nextDocument();
			d1 = it1.nextDocument();
			assertEquals( d0, d1 );
			if ( d0 == END_OF_LIST || d1 == END_OF_LIST ) break;
			documents.add( d0 );
			ReferenceSet<Index> indices0 = it0.indices();
			ReferenceSet<Index> indices1 = it1.indices();
			assertEquals( indices0, indices1 );
			for ( Index index: indices0 ) {
				if ( ! index.hasPositions ) continue;
				IntervalIterator intervalIterator0 = it0.intervalIterator( index );
				IntervalIterator intervalIterator1 = it1.intervalIterator( index );
				assertFalse( indexName.get( index ) + " " + intervalIterator0 + " != " + intervalIterator1, intervalIterator0 == IntervalIterators.FALSE && intervalIterator1 != IntervalIterators.FALSE );
				assertFalse( indexName.get( index ) + " " + intervalIterator0 + " != " + intervalIterator1, intervalIterator0 != IntervalIterators.FALSE && intervalIterator1 == IntervalIterators.FALSE );
				assertFalse( indexName.get( index ) + " " + intervalIterator0 + " != " + intervalIterator1, intervalIterator0 == IntervalIterators.TRUE && intervalIterator1 != IntervalIterators.TRUE );
				assertFalse( indexName.get( index ) + " " + intervalIterator0 + " != " + intervalIterator1, intervalIterator0 != IntervalIterators.TRUE && intervalIterator1 == IntervalIterators.TRUE );
				if ( intervalIterator0 == IntervalIterators.TRUE || intervalIterator1 == IntervalIterators.FALSE ) continue;
				assertEquals( Boolean.valueOf( intervalIterator0 == IntervalIterators.FALSE ), Boolean.valueOf( intervalIterator1 == IntervalIterators.FALSE ) );
				Interval interval0, interval1;
				while ( ( interval0 = intervalIterator0.nextInterval() ) != null && ( interval1 = intervalIterator1.nextInterval() ) != null ) {
					assertEquals( interval0, interval1 );
				}
				assertEquals( Boolean.valueOf( intervalIterator0 == IntervalIterators.FALSE ), Boolean.valueOf( intervalIterator1 == IntervalIterators.FALSE ) );
			}
		}

		if ( maxIter != -1 ) {
			assertEquals( END_OF_LIST, d0 );
			assertEquals( END_OF_LIST, d1 );
		}
		
		return documents;
	}

	private void assertFirstSkip( LongSortedSet documents, DocumentIterator it0, DocumentIterator it1 ) throws IOException {
		assertEquals( documents.firstLong(), it0.skipTo( documents.firstLong() ) );
		assertEquals( documents.firstLong(), it1.skipTo( documents.firstLong() ) );
		assertSame( it0, it1, 4 );
		it0.dispose();
		it1.dispose();
	}

	private void assertLastSkip( LongSortedSet documents, DocumentIterator it0, DocumentIterator it1 ) throws IOException {
		assertEquals( documents.lastLong(), it0.skipTo( documents.lastLong() ) );
		assertEquals( documents.lastLong(), it1.skipTo( documents.lastLong() ) );
		assertSame( it0, it1 );
		it0.dispose();
		it1.dispose();
	}

	private void assertSkipExisting( LongSortedSet documents, DocumentIterator it0, DocumentIterator it1 ) throws IOException {
		long[] document = documents.toLongArray();
		final long d0 = document[ random.nextInt( 1 + document.length / 2 ) ];
		final long d1 = document[ ( document.length - 1 ) / 2 + random.nextInt( 1 + document.length / 2 ) ];
		assertEquals( d0, it0.skipTo( d0 ) );
		assertEquals( d0, it1.skipTo( d0 ) );
		assertSame( it0, it1, 2 );
		assertEquals( it0.skipTo( d1 ), it1.skipTo( d1 ) );
		if ( it0.document() != END_OF_LIST ) assertSame( it0, it1, 2 );
		it0.dispose();
		it1.dispose();
	}

	private void assertSkipNonExisting( LongSortedSet documents, DocumentIterator it0, DocumentIterator it1 ) throws IOException {
		final long lastDoc = documents.lastLong();
		if ( documents.size() < lastDoc + 1 ) {
			long r;
			while( documents.contains( r = ( random.nextLong() & 0x7FFFFFFFFFFFFFFFL ) % ( lastDoc + 1 ) ) );
			assertEquals( it0.skipTo( r ), it1.skipTo( r ) );
			assertSame( it0, it1, 2 );
		}
		it0.dispose();
		it1.dispose();
	}

	public void testQuery( Query query ) throws QueryBuilderVisitorException, IOException {
		DocumentIteratorBuilderVisitor real = new DocumentIteratorBuilderVisitor( indexMap, index[ 0 ], nDocuments * 2 );
		it.unimi.di.big.mg4j.mock.search.DocumentIteratorBuilderVisitor mock = new it.unimi.di.big.mg4j.mock.search.DocumentIteratorBuilderVisitor( indexMap, index[ 0 ], nDocuments * 2 );
		LongSortedSet documents = assertSame( query.accept( mock ), query.accept( real ) );
		if ( ! documents.isEmpty() ) {
			assertFirstSkip( documents, query.accept( mock ), query.accept( real ) );
			assertLastSkip( documents, query.accept( mock ), query.accept( real ) );
			assertSkipExisting( documents, query.accept( mock ), query.accept( real ) );
			assertSkipExisting( documents, query.accept( mock ), query.accept( real ) );
			assertSkipNonExisting( documents, query.accept( mock ), query.accept( real ) );
		}
	}

	public void printResult( Query query, boolean mock, Index indx ) throws QueryBuilderVisitorException, IOException {
		printResult( query, mock, indx, Integer.MAX_VALUE );
	}
	
	public void printResult( Query query, boolean mock, Index indx, int maxDocument ) throws QueryBuilderVisitorException, IOException {
		DocumentIterator it;
		if ( mock ) 
			it = query.accept( new it.unimi.di.big.mg4j.mock.search.DocumentIteratorBuilderVisitor( indexMap, index[ 0 ], nDocuments * 2 ) );
		else
			it = query.accept( new DocumentIteratorBuilderVisitor( indexMap, index[ 0 ], nDocuments * 2 ) );
		long d;
		while ( ( d = it.nextDocument() ) < maxDocument ) {
			System.err.println( "Document: " + d );
			if ( it.intervalIterator( indx ) == IntervalIterators.TRUE )
				System.err.println( indexName.get( indx ) + " --> TRUE" );
			else if ( it.intervalIterator( indx ) == IntervalIterators.FALSE )
				System.err.println( indexName.get( indx ) + " --> FALSE" );
			else
				System.err.println( indexName.get( indx ) + " --> " + IntervalIterators.pour( it.intervalIterator( indx ) ) );
		}
	}

	/**
	 * 
	 * @param level the maximum depth of the query to be generated.
	 * @param canSelect <code>true</code> if {@link Select} is allowed.
	 * @param needsPositions <code>true</code> if all subqueries must be on indices with positions.
	 * @param noPositions <code>true</code> if the current index does not contain positions (hence, no operator
	 *      requiring indices is allowed).
	 * @return an artificially generated query.
	 */
	public Query generateQuery( int level, boolean canSelect, boolean needsPositions, boolean noPositions ) {
		assert level >= 0;
		if ( level == 0 ) return new Term( dictionary[ random.nextInt( dictionary.length ) ] );
		int queryType = random.nextInt( 16 );
		switch( queryType ) {
			case 0: 
			case 1:
			case 2:
			case 3:
			case 12:
				int c = 1 + random.nextInt( maxSubqueries );
				Query q[] = new Query[ c ];
				for ( int i = 0; i < c; i++ ) 
					q[ i ] = generateQuery( level - 1, 
						canSelect && queryType < 2, 
						needsPositions || queryType ==2 || queryType == 3 || queryType == 12, noPositions );
				if ( noPositions && queryType > 1 ) queryType %= 2; //Do not generate Consecutive or OrderedAnds if no positions are available
				switch ( queryType ) {
					case 0: return new Or( q );
					case 1: return new And( q );
					case 2: return new Consecutive( q );
					case 3: return new OrderedAnd( q );
					case 12: 
						int[] gap = new int[ c ];
						for ( int i = 0; i < c; i++ ) gap[ i ] = random.nextInt( maxGap );
						return new Consecutive( q, gap );
				}
			case 4:
				return new Not( generateQuery( level - 1, canSelect, needsPositions, noPositions ) );
			case 5:
				if ( noPositions ) 
					return new Not( generateQuery( level - 1, canSelect, needsPositions, noPositions ) );
				else
					return new Align( generateQuery( level - 1, false, true, noPositions ), generateQuery( level - 1, false, true, noPositions ) );
			case 6:
				if ( canSelect ) 
					if ( random.nextInt( 5 ) == 4 && !needsPositions )
						return new Select( "nonposindex" + random.nextInt( nNonposIndices ), generateQuery( level - 1, canSelect, needsPositions, true ) );
					else
						return new Select( "index" + random.nextInt( nIndices ), generateQuery( level - 1, canSelect, needsPositions, false ) );
			case 7: 
				if ( !noPositions )
					return new Difference( generateQuery( level - 1, canSelect, true, noPositions ), generateQuery( level - 1, canSelect, true, noPositions ), random.nextInt( maxMargin ), random.nextInt( maxMargin ) );
			case 8:
				if ( !noPositions )
					return new LowPass( generateQuery( level - 1, canSelect, true, noPositions ), minLow + random.nextInt( maxLow - minLow ) );
			case 9:
				if ( !noPositions )
					return new Inclusion( generateQuery( level - 1, false, true, noPositions ), generateQuery( level - 1, false, true, noPositions ), random.nextInt( maxMargin ), random.nextInt( maxMargin ) );				
			case 10:
				if ( !noPositions )
					return new Containment( generateQuery( level - 1, false, true, noPositions ), generateQuery( level - 1, false, true, noPositions ), random.nextInt( maxMargin ), random.nextInt( maxMargin ) );				
			case 11:
				return new True();
			case 13:
				return new Term( dictionary[ random.nextInt( dictionary.length ) ] );
			case 14:
				int t = 1 + random.nextInt( dictionary.length - 1 );
				IntOpenHashSet queryTerms = new IntOpenHashSet();
				while ( queryTerms.size() < t ) queryTerms.add( random.nextInt( dictionary.length ) );
				Term tt[] = new Term[ t ];
				int ss[] = new int[ t ];
				queryTerms.toArray( ss );
				for ( int i = 0; i < t; i++ ) tt[ i ] = new Term( dictionary[ ss[ i ] ] );
				return new MultiTerm( tt );
			case 15: 
				DateFormat dateFormat = DateFormat.getDateInstance( DateFormat.SHORT, Locale.UK );
				String dateFrom = dateFormat.format( new Date( random.nextLong() % 1000000000L ) );
				String dateTo = dateFormat.format( new Date( 500000000L + random.nextLong() % 500000000L ) );
				if ( canSelect && !needsPositions ) 
					return new Select( "special" + random.nextInt( nSpecialIndices ), 
						new Range( dateFrom, dateTo ) );
				else 
					return new Term( dictionary[ random.nextInt( dictionary.length ) ] );
				
		}
		return null;
	}
	
	@Test
	public void testOne() throws Exception {
		Query q;
		// (AND<(b, c, #TRUE, (#TRUE <- [[1:2]] #TRUE), "(OR(b, b, b) - [[1:2]] AND(c, a, c, a, c))") -> [[1:2]] (c)~7)

		q = new SimpleParser( new ObjectOpenHashSet<String>( new String[] { "index0", "index1", "index2", "special0" } ), "index0" ).parse(
				//"(b < c < #TRUE < (#TRUE <- [[1:2]] #TRUE) < \"((b|b|b) - [[1:2]] (c a c a c))\") -> [[1:2]] (c)~7"
				"#TRUE <- #TRUE" );
		//q = (Query)BinIO.loadObject("/tmp/q");
		// q = new Consecutive( new Query[] { new Term( "a" ), new Term( "c" ) }, new int[] { 3 , 4 } );
		System.out.println( q );
		//System.out.println( IOUtils.toString( (Reader) specialCollection[ 0 ].document( 0 ).content( 0 ) ) );
		System.out.println( specialCollection[ 0 ].document( 0 ).content( 0 ) );
		testQuery( q );
		printResult( q, false, index[ nIndices ], 1 );
	}

	
	public void testSimple() throws QueryBuilderVisitorException, IOException {
		for ( int i = 0; i < 2000; i++ ) {
			Query q = generateQuery( random.nextInt( 6 ), true, false, false );
			//it.unimi.dsi.fastutil.io.BinIO.storeObject( q, "/tmp/query" );
			System.out.println( q );
			testQuery( q );
		}
	}

	@Test
	public void testRandom() throws QueryBuilderVisitorException, IOException {
		for ( int i = 0; i < 1000; i++ ) {
			Query q = generateQuery( random.nextInt( 6 ), true, false, false );
			System.out.println( q );
			testQuery( q );
		}
	}
}
