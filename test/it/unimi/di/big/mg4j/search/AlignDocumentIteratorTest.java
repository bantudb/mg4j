package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.util.Interval;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AlignDocumentIteratorTest {
	
	private static Object2ReferenceMap<String,Index> indexMap;
	private static Index index1;
	private static Index index2;
	private static SimpleParser parser;
	private static File tempDir;
	private static Index index3;

	@BeforeClass
	public static void prepareIndices() throws IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		tempDir = File.createTempFile( "mg4jtest", null );
		tempDir.delete();
		tempDir.mkdir();

		String basename1 = File.createTempFile( AlignDocumentIterator.class.getSimpleName(), "index1", tempDir ).toString();
		String basename2 = File.createTempFile( AlignDocumentIterator.class.getSimpleName(), "index2", tempDir ).toString();
		String basename3 = File.createTempFile( AlignDocumentIterator.class.getSimpleName(), "index3", tempDir ).toString();


		StringArrayDocumentCollection index1DocumentCollection = new StringArrayDocumentCollection( 
				new String[] { 
						"Washington was in Washington", 
						"I go and play go",
						"go is difficult", 
						"if you play then the players go"
				}
		); 

		StringArrayDocumentCollection index2DocumentCollection = new StringArrayDocumentCollection( 
				new String[] { 
						"name verb preposition name", 
						"pronoun verb conjunction verb name",
						"name verb adjective",
						"adverb pronoun verb conjunction article name verb"
				}
		); 

		StringArrayDocumentCollection index3DocumentCollection = new StringArrayDocumentCollection( 
				new String[] { 
						"person NA NA city", 
						"person NA NA NA object",
						"object NA NA",
						"NA person NA NA NA person NA"
				}
		); 

		new IndexBuilder( basename1, index1DocumentCollection ).run();
		new IndexBuilder( basename2, index2DocumentCollection ).run();
		new IndexBuilder( basename3, index3DocumentCollection ).run();
		index1 = Index.getInstance( basename1 + "-text", true, true );
		index2 = Index.getInstance( basename2 + "-text", true, true );
		index3 = Index.getInstance( basename3 + "-text", true, true );

		indexMap = new Object2ReferenceOpenHashMap<String,Index>( new String[] { "index1", "index2", "index3" }, new Index[] { index1, index2, index3 } );
		Object2ReferenceOpenHashMap<String, TermProcessor> termProcessors = new Object2ReferenceOpenHashMap<String,TermProcessor>();
		for( String s: indexMap.keySet() ) termProcessors.put( s, indexMap.get( s ).termProcessor );
		parser = new SimpleParser( 
				indexMap.keySet(),
				"index1", 
				termProcessors
		);
	}

	@AfterClass
	public static void deleteIndices() throws IOException {
			FileUtils.forceDelete( tempDir );
	}
	
	private DocumentIterator executeQuery( String q ) throws QueryParserException, QueryBuilderVisitorException {
		it.unimi.di.big.mg4j.query.nodes.Query query = parser.parse( q );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( indexMap, index1, Integer.MAX_VALUE );
		return query.accept( documentIteratorBuilderVisitor );
	}
	
	@Test
	public void oneIndexTest() throws QueryParserException, QueryBuilderVisitorException, IOException {
		DocumentIterator docIt;
		IntervalIterator intIt1, intIt2, intIt3;
		
		docIt = executeQuery( "Washington" );
		assertTrue( docIt.mayHaveNext() ); 
		assertEquals( 0, docIt.nextDocument() );
		intIt1 = docIt.intervalIterators().get( index1 );
		assertNull( docIt.intervalIterators().get( index2 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 0 ), Interval.valueOf( 3 ) }, IntervalIterators.pour( intIt1 ).toArray() );
		
		docIt = executeQuery( "play go" );
		assertEquals( 1, docIt.nextDocument() );
		intIt1 = docIt.intervalIterators().get( index1 );
		assertNull( docIt.intervalIterators().get( index2 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 1, 3 ), Interval.valueOf( 3, 4 ) }, IntervalIterators.pour( intIt1 ).toArray() );
		assertEquals( 3, docIt.nextDocument() );
		assertNull( docIt.intervalIterators().get( index2 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		intIt1 = docIt.intervalIterators().get( index1 );
		assertArrayEquals( new Interval[] { Interval.valueOf( 2, 6 ) }, IntervalIterators.pour( intIt1 ).toArray() );
		
		docIt = executeQuery( "index2:(name verb)" );
		assertEquals( 0, docIt.nextDocument() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 0, 1 ), Interval.valueOf( 1, 3 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		assertEquals( 1, docIt.nextDocument() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 3, 4 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		assertEquals( 2, docIt.nextDocument() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 0, 1 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		assertEquals( 3, docIt.nextDocument() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 2, 5 ), Interval.valueOf( 5, 6 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		
		docIt = executeQuery( "index3:city" );
		assertTrue( docIt.mayHaveNext() ); 
		assertEquals( 0, docIt.nextDocument() );
		intIt3 = docIt.intervalIterators().get( index3 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index2 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 3 ) }, IntervalIterators.pour( intIt3 ).toArray() );
	}
	
	
	@Test
	public void alignTestSmall() throws QueryParserException, QueryBuilderVisitorException, IOException {
		DocumentIterator docIt;
		IntervalIterator intIt1, intIt2, intIt3;
		
		docIt = executeQuery( "Washington ^ index3:city" );
		assertTrue( docIt.mayHaveNext() ); 
		assertEquals( 0, docIt.nextDocument() );
		intIt1 = docIt.intervalIterators().get( index1 );
		assertNull( docIt.intervalIterators().get( index2 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertEquals( 1, docIt.indices().size() );
		assertArrayEquals( new Interval[] { Interval.valueOf( 3 ) }, IntervalIterators.pour( intIt1 ).toArray() );
		
		docIt = executeQuery( "index3:city ^ Washington" );
		assertTrue( docIt.mayHaveNext() ); 
		assertEquals( 0, docIt.nextDocument() );
		intIt3 = docIt.intervalIterators().get( index3 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index2 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 3 ) }, IntervalIterators.pour( intIt3 ).toArray() );

		docIt = executeQuery( "index2:name ^ index3:person" );
		assertTrue( docIt.mayHaveNext() ); 
		assertEquals( 0, docIt.nextDocument() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 0 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		assertEquals( 3, docIt.nextDocument() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 5 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		assertEquals( END_OF_LIST, docIt.nextDocument() );

		docIt.dispose();
	}
	
	@Test
	public void skipToTest() throws QueryParserException, QueryBuilderVisitorException, IOException {
		DocumentIterator docIt;
		IntervalIterator intIt2;
		
		docIt = executeQuery( "index2:verb ^ index3:NA" );
		assertEquals( -1, docIt.skipTo( -5 ) ); // And does nothing
		assertEquals( 0, docIt.nextDocument() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 1 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		assertEquals( 0, docIt.skipTo( 0 ) ); // And does nothing, since k=0 and n=0, so k>=n 
		assertEquals( 3, docIt.skipTo( 3 ) ); // Skips to 3
		assertEquals( 3, docIt.document() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 2 ), Interval.valueOf( 6 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		
		LongSet terms = new LongOpenHashSet();
		intIt2.intervalTerms( terms );
		LongSet expectedTerms = new LongOpenHashSet();
		expectedTerms.add( index2.termMap.getLong( "verb" ) );   
		assertEquals( expectedTerms, terms );
		
		assertEquals( 3, docIt.skipTo( 3 ) ); // And does nothing, since k=3 and n=3, so k>=n
		assertEquals( DocumentIterator.END_OF_LIST, docIt.skipTo( END_OF_LIST ) ); 
		assertEquals( DocumentIterator.END_OF_LIST, docIt.skipTo( 3 ) ); // Last call to skipTo returned MAX_VALUE, so this call should do the same!
	}
	
	@Test
	public void noIndexIterators() throws QueryParserException, QueryBuilderVisitorException, IOException {
		DocumentIterator docIt;
		IntervalIterator intIt2;
		
		docIt = executeQuery( "index2:(name verb) ^ index3:(person NA)" );
		//assertEquals( END_OF_LIST, docIt.skipTo( -5 ) ); // And does nothing
		assertTrue( docIt.mayHaveNext() );
		assertEquals( 0, docIt.nextDocument() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );
		assertArrayEquals( new Interval[] { Interval.valueOf( 0, 1 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		assertEquals( 0, docIt.skipTo( 0 ) ); // And does nothing, since k=0 and n=0, so k>=n 
		assertEquals( 3, docIt.skipTo( 3 ) ); // Skips to 3
		assertEquals( 3, docIt.document() );
		intIt2 = docIt.intervalIterators().get( index2 );
		assertNull( docIt.intervalIterators().get( index1 ) );
		assertNull( docIt.intervalIterators().get( index3 ) );

		LongSet expectedTerms = new LongOpenHashSet();
		expectedTerms.add( (int) index2.termMap.getLong( "name" ) ); 
		expectedTerms.add( (int) index2.termMap.getLong( "verb" ) ); 
		LongSet terms = new LongOpenHashSet();
		intIt2.intervalTerms( terms );
		assertEquals( expectedTerms, terms );
		
		assertArrayEquals( new Interval[] { Interval.valueOf( 5, 6 ) }, IntervalIterators.pour( intIt2 ).toArray() );
		assertEquals( 3, docIt.skipTo( 3 ) ); // And does nothing, since k=3 and n=3, so k>=n

		assertEquals( DocumentIterator.END_OF_LIST, docIt.skipTo( END_OF_LIST ) ); 
		assertEquals( DocumentIterator.END_OF_LIST, docIt.skipTo( 3 ) ); // Last call to skipTo returned MAX_VALUE, so this call should do the same!
	}
}
