package it.unimi.di.big.mg4j.search.score;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.visitor.CounterCollectionVisitor;
import it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor;
import it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor;
import it.unimi.di.big.mg4j.tool.Combine.IndexType;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.di.big.mg4j.tool.Paste;
import it.unimi.dsi.big.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.big.util.SemiExternalGammaBigList;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.io.InputBitStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ScorerTest {
	/** The temporary directory where all tests are run. */
	private static File tempDir;

	private static StringArrayDocumentCollection documentCollection0;

	private static StringArrayDocumentCollection bodyDocumentCollection;

	private static StringArrayDocumentCollection titleDocumentCollection;

	private static StringArrayDocumentCollection bodyFDocumentCollection;

	private static StringArrayDocumentCollection titleFDocumentCollection;
	private static final double ASSERT_DIFF = 1E-3;
	private static String basename0;

	private static String bodyFBasename;

	private static String titleFBasename;

	private static String bodyBasename;

	private static String titleBasename;

	private static String basenameFComb;

	private static String bodyBasenameBis;
	private static Index index0;
	private static Index indexFBody;
	private static Index indexFTitle;
	private static Index indexBody;

	@SuppressWarnings("unused")
	private static
	Index indexTitle;
	private static Index indexBodyBis;
	private static SimpleParser simpleParser;
	private static ImmutableExternalPrefixMap immutableExternalPrefixMap;

	
	/**
	 * Prepare the index of documents.
	 */

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		// Create a new directory under /tmp
		tempDir = File.createTempFile( "mg4jtest", null );
		tempDir.delete();
		tempDir.mkdir();

		basename0 = File.createTempFile( ScorerTest.class.getSimpleName(), "test0", tempDir ).toString();
		bodyBasename = File.createTempFile( ScorerTest.class.getSimpleName(), "body", tempDir ).toString();
		titleBasename = File.createTempFile( ScorerTest.class.getSimpleName(), "title", tempDir ).toString();
		bodyFBasename = File.createTempFile( ScorerTest.class.getSimpleName(), "bodyf", tempDir ).toString();
		titleFBasename = File.createTempFile( ScorerTest.class.getSimpleName(), "titlef", tempDir ).toString();
		basenameFComb = File.createTempFile( ScorerTest.class.getSimpleName(), "combf", tempDir ).toString();
		bodyBasenameBis = File.createTempFile( ScorerTest.class.getSimpleName(), "bodyfbis", tempDir ).toString();
		documentCollection0 = new StringArrayDocumentCollection( 
				// number of documents N = 3
				// average document size = 9 + 4 + 14 / 3 = 9
				// number of occurrences = 27
				new String[] { 
						"This sentence speaks really really really good of gods", // size 9
						"And this, is a list of all the green things in the green world", // size 14
						"get THIS not THAT" } // size 4 
				); 

		// BM25 testing

		bodyDocumentCollection = new StringArrayDocumentCollection( 
				new String[] { 
						"A C C", 
						"D Z",
						"A X Z", 
						"C X X",
						"Q",
				}
		); 
		
		// Doc size: 3 2 3 3 1 (avg: 12/5)
		// Frequency: A:2 C:2 D:1 Q:1 X:2 Z:2

		titleDocumentCollection = new StringArrayDocumentCollection( 
				new String[] { 
						"A", 
						"Q",
						"Z", 
						"Q X",
						"Z",
				}
		); 
		
		// Doc size: 1 1 1 2 1 (avg: 9/8)
		// Frequency: A:1 Q:2 X:1 Z:2
		 
		// BM25F testing

		bodyFDocumentCollection = new StringArrayDocumentCollection( 
				new String[] { 
						"A C C", 
						"D Z",
						"A X Z", 
						"C X X",
						"Q",
						"1",
						"1",
						"1"
				}
		); 
		
		// Doc size: 3 2 3 3 1 (avg: 12/5)
		// Frequency: A:2 C:2 D:1 Q:1 X:2 Z:2

		titleFDocumentCollection = new StringArrayDocumentCollection( 
				new String[] { 
						"A", 
						"Q",
						"Z", 
						"Q X",
						"Z",
						"1",
						"1",
						"1"
				}
		); 
		
		// Doc size: 1 1 1 2 1 (avg: 9/8)
		// Frequency: A:1 Q:2 X:1 Z:2

		// Global data for BM25F
		// Weight: coll2:3, coll1:1
		// Doc size: 6 5 6 9 4 (avg: 15/8)
		// Frequency: A:2 C:2 D:1 Q:3 X:2 Z:3
		
		new IndexBuilder( basename0, documentCollection0 ).run();
		new IndexBuilder( bodyBasename, bodyDocumentCollection ).run();
		new IndexBuilder( titleBasename, titleDocumentCollection ).run();
		new IndexBuilder( bodyFBasename, bodyFDocumentCollection ).run();
		new IndexBuilder( titleFBasename, titleFDocumentCollection ).run();
		new IndexBuilder( bodyBasenameBis, bodyDocumentCollection ).run();
		index0 = Index.getInstance( basename0 + "-text", true, true );
		indexBody = Index.getInstance( bodyBasename + "-text", true, true );
		indexTitle = Index.getInstance( titleBasename + "-text", true, true );
		indexFBody = Index.getInstance( bodyFBasename + "-text", true, true );
		indexFTitle = Index.getInstance( titleFBasename + "-text", true, true );
		indexBodyBis = Index.getInstance( bodyBasenameBis + "-text", true, true );

		new Paste( IOFactory.FILESYSTEM_FACTORY, basenameFComb, new String[] { bodyFBasename + "-text", titleFBasename + "-text" }, false, true, 1024, null, 1024, CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX, IndexType.QUASI_SUCCINCT, false, -1, -1, -1, 10 ).run();
		immutableExternalPrefixMap = new ImmutableExternalPrefixMap( new FileLinesCollection( basenameFComb + ".terms", "UTF-8" ) );
		simpleParser = new SimpleParser( index0.termProcessor );
	}

	@AfterClass
	public static void tearDown() throws IOException {
		FileUtils.forceDelete( tempDir );
	}
	
	private void assertScores( String q, it.unimi.di.big.mg4j.index.Index index, Scorer scorer, double[] expected ) throws QueryParserException, QueryBuilderVisitorException, IOException {
		it.unimi.di.big.mg4j.query.nodes.Query query = simpleParser.parse( q );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, index, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		scorer.wrap( documentIterator );
		
		int i = 0;
		while ( documentIterator.nextDocument() != END_OF_LIST ) {
			final double score = scorer.score();
			assertEquals( "Item " + i + " (document " + documentIterator.document() + ")", expected[ i ], score, ASSERT_DIFF );
			i++;
		}
		
		documentIterator.dispose();
	}

	private void assertScoresMultiIndex( String q, Object2ReferenceMap<String,it.unimi.di.big.mg4j.index.Index> indexMap, it.unimi.di.big.mg4j.index.Index defaultIndex, Scorer scorer, double[] expected ) throws QueryParserException, QueryBuilderVisitorException, IOException {
		Object2ReferenceOpenHashMap<String, TermProcessor> termProcessors = new Object2ReferenceOpenHashMap<String,TermProcessor>();
		for( String s: indexMap.keySet() ) termProcessors.put( s, indexMap.get( s ).termProcessor );
		SimpleParser simpleParser = new SimpleParser( 
			indexMap.keySet(),
			"t", // TODO: make it a parameter
			termProcessors
		);
				
		it.unimi.di.big.mg4j.query.nodes.Query query = simpleParser.parse( q );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( indexMap, defaultIndex, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		scorer.wrap( documentIterator );
		
		int i = 0;
		while ( documentIterator.nextDocument() != END_OF_LIST ) {
			final double score = scorer.score();
			assertEquals( "Item " + i + " (document " + documentIterator.document() + ")", expected[ i ], score, ASSERT_DIFF );
			i++;
		}
		
		assertEquals( i, expected.length );
		
		documentIterator.dispose();
	}

	@Test
	public void testBM25Scorer1() throws Exception {
		Scorer scorer = new BM25Scorer( .75, .95 );

		assertScores( "this", index0, scorer, new double[] { 0, 0, 0 } );
		assertScores( "list & things", index0, scorer, new double[] { .8332 } );
		assertScores( "good | green", index0, scorer, new double[] { .5108, .5683 } );

		// This is an ugly trick to force the non-optimized evaluation to take place.
		assertScores( "(this)~1000", index0, scorer, new double[] { 0, 0, 0 } );
		assertScores( "(list & things)~1000", index0, scorer, new double[] { .8332 } );
		assertScores( "(good | green)~1000", index0, scorer, new double[] { .5108, .5683 } );
	}

	
	@Test
	public void testBM25Scorer4() throws Exception {
		Scorer scorer = new BM25Scorer( .75, .95 );

		assertScores( "A", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "A | W", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "W | A", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "A & A", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "A | A", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "C", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "C | W", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "W | C", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "C & C", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "C | C", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "Q", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "Q | W", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "W | Q", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "Q & Q", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "Q | Q", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "A & C", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "A & C | W", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "W | A & C", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(A & C) & (A & C)", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(A & C) | (A & C)", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(A & C)~2", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(A & C)~2 | W", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "W | (A & C)~2", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(A & C)~2 & (A & C)~2", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(A & C)~2 | (A & C)~2", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "A & X & Z", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "A & X & Z | W", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "W | A & X & Z", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "(A & X & Z) & (A & X & Z)", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "(A & X & Z) | (A & X & Z)", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "C | A", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "C | A | W", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "W | C | A", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "(C | A) & (C | A)", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "(C | A) | (C | A)", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "X | Z", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );
		assertScores( "X | Z | W", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );
		assertScores( "W | X | Z", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );
		assertScores( "(X | Z) & (X | Z)", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );
		assertScores( "(X | Z) | (X | Z)", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );

		// This is an ugly trick to force the non-optimized evaluation to take place.
		assertScores( "(A)~1000", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "(A | W)~1000", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "(W | A)~1000", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "(A & A)~1000", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "(A | A)~1000", indexBody, scorer, new double[] { .3053, .3053 } );
		assertScores( "(C)~1000", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "(C | W)~1000", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "(W | C)~1000", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "(C & C)~1000", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "(C | C)~1000", indexBody, scorer, new double[] { .4021, .3053 } );
		assertScores( "(Q)~1000", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "(Q | W)~1000", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "(W | Q)~1000", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "(Q & Q)~1000", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "(Q | Q)~1000", indexBody, scorer, new double[] { 1.4408 } );
		assertScores( "(A & C)~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(A & C | W)~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(W | A & C)~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "((A & C) & (A & C))~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "((A & C) | (A & C))~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "((A & C)~2)~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "((A & C)~2 | W)~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(W | (A & C)~2)~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "((A & C)~2 & (A & C)~2)~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "((A & C)~2 | (A & C)~2)~1000", indexBody, scorer, new double[] { .3053 + .4021 } );
		assertScores( "(A & X & Z)~1000", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "(A & X & Z | W)~1000", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "(W | A & X & Z)~1000", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "((A & X & Z) & (A & X & Z))~1000", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "((A & X & Z) | (A & X & Z))~1000", indexBody, scorer, new double[] { .3053 + .3053 + .3053 } );
		assertScores( "(C | A)~1000", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "(C | A | W)~1000", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "(W | C | A)~1000", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "((C | A) & (C | A))~1000", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "((C | A) | (C | A))~1000", indexBody, scorer, new double[] { .4021 + .3053, .3053, .3053 } );
		assertScores( "(X | Z)~1000", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );
		assertScores( "(X | Z | W)~1000", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );
		assertScores( "(W | X | Z)~1000", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );
		assertScores( "((X | Z) & (X | Z))~1000", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );
		assertScores( "((X | Z) | (X | Z))~1000", indexBody, scorer, new double[] { .3609, .3053 + .3053, .4021 } );
	}

	@Test
	public void testBM25Scorer5() throws Exception {
		Scorer scorer = new BM25Scorer( .75, .95 );

		assertScores( "A + X", indexBody, scorer, new double[] { .3053, .4021, .4021 } );
		assertScores( "D + Z", indexBody, scorer, new double[] { 0.447, .3053 } );
	}
	
	@Test
	public void testBM25ScorerMulti() throws Exception {
		Scorer scorer = new BM25Scorer( .75, .95 );
		scorer.setWeights( new Reference2DoubleOpenHashMap<Index>( new Index[] { indexBody, indexBodyBis }, new double[] { 1, 3 } ) );
		Object2ReferenceMap<String,Index> indexMap = new Object2ReferenceOpenHashMap<String,Index>( new String[] { "t", "u" }, new Index[] { indexBody, indexBodyBis } );
		
		assertScoresMultiIndex( "A", indexMap, indexBody, scorer, new double[] { .3053 * .25, .3053 * .25 } );
		assertScoresMultiIndex( "u:A", indexMap, indexBody, scorer, new double[] { .3053 * .75, .3053 * .75 } );
		assertScoresMultiIndex( "t:A", indexMap, indexBody, scorer, new double[] { .3053 * .25, .3053 * .25 } );
		assertScoresMultiIndex( "u:A & t:A", indexMap, indexBody, scorer, new double[] { .3053, .3053 } );
		assertScoresMultiIndex( "u:A | t:A", indexMap, indexBody, scorer, new double[] { .3053, .3053 } );
		assertScoresMultiIndex( "u:A & t:C", indexMap, indexBody, scorer, new double[] { .3053 * .75 + .4021 * .25 } );
		assertScoresMultiIndex( "t:A & u:C", indexMap, indexBody, scorer, new double[] { .3053 * .25 + .4021 * .75 } );
		assertScoresMultiIndex( "u:A | t:C", indexMap, indexBody, scorer, new double[] { .3053 * .75 + .4021 * .25, .3053 * .75, .3053 *.25 } );
		assertScoresMultiIndex( "t:A | u:C", indexMap, indexBody, scorer, new double[] { .3053 *.25 + .4021 *.75, .3053 *.25, .3053 * .75 } );
		assertScoresMultiIndex( "u:A | (u:A & t:C)", indexMap, indexBody, scorer, new double[] { .3053 * .75 + .4021 * .25, .3053 * .75 } );
		assertScoresMultiIndex( "t:A | (u:A & t:C)", indexMap, indexBody, scorer, new double[] { .3053 * .25+ .3053 * .75 + .4021 *.25, .3053 *.25 } );
		assertScoresMultiIndex( "t:X & (t:A | u:C)", indexMap, indexBody, scorer, new double[] { .3053 *.25 + .3053 *.25 , .4021 *.25+ .3053 * .75 } );
		assertScoresMultiIndex( "u:X & (u:A | t:C)", indexMap, indexBody, scorer, new double[] { .3053 * .75 + .3053 * .75, .4021 * .75 + .3053 *.25 } );

		// This is an ugly trick to force the non-optimized evaluation to take place.
		assertScoresMultiIndex( "(A)~1000", indexMap, indexBody, scorer, new double[] { .3053 * .25, .3053 * .25 } );
		assertScoresMultiIndex( "(u:A)~1000", indexMap, indexBody, scorer, new double[] { .3053 * .75, .3053 * .75 } );
		assertScoresMultiIndex( "(t:A)~1000", indexMap, indexBody, scorer, new double[] { .3053 * .25, .3053 * .25 } );
		assertScoresMultiIndex( "(u:A & t:A)~1000", indexMap, indexBody, scorer, new double[] { .3053, .3053 } );
		assertScoresMultiIndex( "(u:A | t:A)~1000", indexMap, indexBody, scorer, new double[] { .3053, .3053 } );
		assertScoresMultiIndex( "(u:A & t:C)~1000", indexMap, indexBody, scorer, new double[] { .3053 * .75 + .4021 * .25 } );
		assertScoresMultiIndex( "(t:A & u:C)~1000", indexMap, indexBody, scorer, new double[] { .3053 * .25 + .4021 * .75 } );
		assertScoresMultiIndex( "(u:A | t:C)~1000", indexMap, indexBody, scorer, new double[] { .3053 * .75 + .4021 * .25, .3053 * .75, .3053 *.25 } );
		assertScoresMultiIndex( "(t:A | u:C)~1000", indexMap, indexBody, scorer, new double[] { .3053 *.25 + .4021 *.75, .3053 *.25, .3053 * .75 } );
		assertScoresMultiIndex( "(u:A | (u:A & t:C))~1000", indexMap, indexBody, scorer, new double[] { .3053 * .75 + .4021 * .25, .3053 * .75 } );
		assertScoresMultiIndex( "(t:A | (u:A & t:C))~1000", indexMap, indexBody, scorer, new double[] { .3053 * .25+ .3053 * .75 + .4021 *.25, .3053 *.25 } );
		assertScoresMultiIndex( "(t:X & (t:A | u:C))~1000", indexMap, indexBody, scorer, new double[] { .3053 *.25 + .3053 *.25 , .4021 *.25+ .3053 * .75 } );
		assertScoresMultiIndex( "(u:X & (u:A | t:C))~1000", indexMap, indexBody, scorer, new double[] { .3053 * .75 + .3053 * .75, .4021 * .75 + .3053 *.25 } );
	}
	
	@Test
	public void testBM25FScorer() throws FileNotFoundException, IOException, QueryParserException, QueryBuilderVisitorException {
		Scorer scorer = new BM25FScorer( .95, new Reference2DoubleOpenHashMap<Index>( new Index[] { indexFBody, indexFTitle }, new double[] { 0.5, 0.3 } ), immutableExternalPrefixMap, new SemiExternalGammaBigList( new InputBitStream( basenameFComb + DiskBasedIndex.FREQUENCIES_EXTENSION ) ) );
		scorer.setWeights( new Reference2DoubleOpenHashMap<Index>( new Index[] { indexFTitle, indexFBody }, new double[] { .85, .15 } ) );
		// b(ody):.3 t(itle):1.7
		Object2ReferenceMap<String,Index> indexMap = new Object2ReferenceOpenHashMap<String,Index>( new String[] { "b", "t" }, new Index[] { indexFBody, indexFTitle } );
		
		assertScoresMultiIndex( "b:A | t:A", indexMap, indexFBody, scorer, new double[] { 0.953, 0.202 } );
		assertScoresMultiIndex( "b:C | t:C", indexMap, indexFBody, scorer, new double[] { .364, .202 } );
		assertScoresMultiIndex( "b:D | t:D", indexMap, indexFBody, scorer, new double[] { 0.416 } );
		assertScoresMultiIndex( "b:X | t:X", indexMap, indexFBody, scorer, new double[] { .202, .917 } );
		assertScoresMultiIndex( "b:Z | t:Z", indexMap, indexFBody, scorer, new double[] { .117, .451, .424 } );
		assertScoresMultiIndex( "b:Q | t:Q", indexMap, indexFBody, scorer, new double[] { .424, .371, .151 } );

		assertScoresMultiIndex( "t:A | b:A", indexMap, indexFBody, scorer, new double[] { 0.953, 0.202 } );
		assertScoresMultiIndex( "t:A | b:A | t:A", indexMap, indexFBody, scorer, new double[] { 0.953, 0.202 } );
		assertScoresMultiIndex( "t:C | b:C", indexMap, indexFBody, scorer, new double[] { .364, .202 } );
		assertScoresMultiIndex( "t:D | b:D", indexMap, indexFBody, scorer, new double[] { 0.416 } );
		assertScoresMultiIndex( "t:X | b:X", indexMap, indexFBody, scorer, new double[] { .202, .917 } );
		assertScoresMultiIndex( "t:Z | b:Z", indexMap, indexFBody, scorer, new double[] { .117, .451, .424 } );
		assertScoresMultiIndex( "t:Q | b:Q", indexMap, indexFBody, scorer, new double[] { .424, .371, .151 } );

		assertScoresMultiIndex( "b:(A|X) | t:(A|X)", indexMap, indexFBody, scorer, new double[] { .953, .404, .917 } );
		assertScoresMultiIndex( "b:(A|B) | t:(A|B)", indexMap, indexFBody, scorer, new double[] { .953, .202 } );
		assertScoresMultiIndex( "b:(A|B|X) | t:(A|B|X)", indexMap, indexFBody, scorer, new double[] { .953, .404, .917 } );
		assertScoresMultiIndex( "b:(Q|X) | t:(Q|X)", indexMap, indexFBody, scorer, new double[] { .424, .202, 1.287, .151 } );
		// Here offset and term-id lists differ.
		assertScoresMultiIndex( "b:(A|A|B|X) | t:(A|B|X)", indexMap, indexFBody, scorer, new double[] { .953, .404, .917 } );
		assertScoresMultiIndex( "b:(A|A|B|X) | t:((X&X)|(B&B)|A)", indexMap, indexFBody, scorer, new double[] { .953, .404, .917 } );
		
		assertScoresMultiIndex( "b:A", indexMap, indexFBody, scorer, new double[] { 0.202, .202 } );
		assertScoresMultiIndex( "b:C", indexMap, indexFBody, scorer, new double[] { .364, .202 } );
		assertScoresMultiIndex( "b:D", indexMap, indexFBody, scorer, new double[] { 0.416 } );
		assertScoresMultiIndex( "b:X", indexMap, indexFBody, scorer, new double[] { .202, .364 } );
		assertScoresMultiIndex( "b:Z", indexMap, indexFBody, scorer, new double[] { .117, .095 } );
		assertScoresMultiIndex( "b:Q", indexMap, indexFBody, scorer, new double[] { .151 } );

		assertScoresMultiIndex( "t:X", indexMap, indexFBody, scorer, new double[] { .783 } );

		assertScoresMultiIndex( "b:C t:X", indexMap, indexFBody, scorer, new double[] { 0.202 + .783 } );
	}

	@Test
	public void testBM25FScorerExpectedIdf() throws FileNotFoundException, IOException, QueryParserException, QueryBuilderVisitorException {
		Scorer scorer = new BM25FScorer( .95, new Reference2DoubleOpenHashMap<Index>( new Index[] { indexFBody, indexFTitle }, new double[] { 0.5, 0.3 } ), null, null );
		scorer.setWeights( new Reference2DoubleOpenHashMap<Index>( new Index[] { indexFTitle, indexFBody }, new double[] { .85, .15 } ) );
		// b(ody):.3 t(itle):1.7
		Object2ReferenceMap<String,Index> indexMap = new Object2ReferenceOpenHashMap<String,Index>( new String[] { "b", "t" }, new Index[] { indexFBody, indexFTitle } );
		
		assertScoresMultiIndex( "b:A | t:A", indexMap, indexFBody, scorer, new double[] { 1.53, 0.202 } );
		assertScoresMultiIndex( "b:C | t:C", indexMap, indexFBody, scorer, new double[] { .364, .202 } );
		assertScoresMultiIndex( "b:D | t:D", indexMap, indexFBody, scorer, new double[] { 0.416 } );
		assertScoresMultiIndex( "b:X | t:X", indexMap, indexFBody, scorer, new double[] { .202, 1.387 } );
		assertScoresMultiIndex( "b:Z | t:Z", indexMap, indexFBody, scorer, new double[] { .247, .953, .896 } );
		assertScoresMultiIndex( "b:Q | t:Q", indexMap, indexFBody, scorer, new double[] { .896, .783, .536 } );

		assertScoresMultiIndex( "b:(A|X) | t:(A|X)", indexMap, indexFBody, scorer, new double[] { 1.530, 0.404, 1.387 } );
		assertScoresMultiIndex( "b:(A|B) | t:(A|B)", indexMap, indexFBody, scorer, new double[] { 1.530, 0.202 } );
		assertScoresMultiIndex( "b:(A|B|X) | t:(A|B|X)", indexMap, indexFBody, scorer, new double[] { 1.530, .404, 1.387 } );
		assertScoresMultiIndex( "b:(Q|X) | t:(Q|X)", indexMap, indexFBody, scorer, new double[] { .896, .202, 2.17, .536 } );
		
		assertScoresMultiIndex( "b:A", indexMap, indexFBody, scorer, new double[] { 0.202, .202 } );
		assertScoresMultiIndex( "b:C", indexMap, indexFBody, scorer, new double[] { .364, .202 } );
		assertScoresMultiIndex( "b:D", indexMap, indexFBody, scorer, new double[] { 0.416 } );
		assertScoresMultiIndex( "b:X", indexMap, indexFBody, scorer, new double[] { .202, .364 } );
		assertScoresMultiIndex( "b:Z", indexMap, indexFBody, scorer, new double[] { .247, .202 } );
		assertScoresMultiIndex( "b:Q", indexMap, indexFBody, scorer, new double[] { .536 } );

		assertScoresMultiIndex( "t:X", indexMap, indexFBody, scorer, new double[] { 1.320 } );

		assertScoresMultiIndex( "b:C t:X", indexMap, indexFBody, scorer, new double[] { 0.202 + 1.320 } );
	}

	
	@Test
	public void testCounterSetup() throws Exception {

		TermCollectionVisitor termVisitor = new TermCollectionVisitor();
		CounterSetupVisitor setupVisitor = new CounterSetupVisitor( termVisitor );
		CounterCollectionVisitor counterCollectionVisitor = new CounterCollectionVisitor( setupVisitor );

		it.unimi.di.big.mg4j.query.nodes.Query query = simpleParser.parse( "this" );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, index0, Integer.MAX_VALUE );

		DocumentIterator indexIterator = query.accept( documentIteratorBuilderVisitor );

		indexIterator.nextDocument();

		termVisitor.prepare();
		indexIterator.accept( termVisitor );
		setupVisitor.prepare();
		indexIterator.accept( setupVisitor );

		// assertEquals( 3, indexIterator.frequency() );
		assertEquals( 3, setupVisitor.frequency[ 0 ] );

		assertEquals( 1, indexIterator.nextDocument() );
		counterCollectionVisitor.prepare();
		indexIterator.accept( counterCollectionVisitor );
		assertEquals( 1, setupVisitor.count[ 0 ] );
		assertEquals( 2, indexIterator.nextDocument() );
		counterCollectionVisitor.prepare();
		indexIterator.accept( counterCollectionVisitor );
		assertEquals( 1, setupVisitor.count[ 0 ] );
	}

	public void testCountScorer() throws QueryParserException, QueryBuilderVisitorException, IOException {

		Scorer scorer = new CountScorer();

		it.unimi.di.big.mg4j.query.nodes.Query query = simpleParser.parse( "C" );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, indexBody, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		scorer.wrap( documentIterator );
		
		final double expected[] = { 2, 1 };

		int i = 0;
		while ( documentIterator.nextDocument() != END_OF_LIST ) {
			final double score = scorer.score();
			assertEquals( "Item " + i, expected[ i ], score, 0.001 );
			i++;
		}
	}
}
