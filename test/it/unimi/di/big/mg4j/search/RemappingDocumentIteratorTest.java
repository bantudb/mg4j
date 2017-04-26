package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.Consecutive;
import it.unimi.di.big.mg4j.query.nodes.Or;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.nodes.Remap;
import it.unimi.di.big.mg4j.query.nodes.Select;
import it.unimi.di.big.mg4j.query.nodes.Term;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;

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

public class RemappingDocumentIteratorTest {
	private static Index index0;
	private static Index index1;
	private static Index index2;
	private static Index index3;
	private static String basename;
	private static Object2ReferenceMap<String, Index> indexMap;

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		basename = File.createTempFile( RemappingDocumentIteratorTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename + "0", new StringArrayDocumentCollection( "a b", "b c", "c" ) ).run();
		new IndexBuilder( basename + "1", new StringArrayDocumentCollection( "a b", "b c", "c" ) ).run();
		index0 = Index.getInstance( basename + "0-text", true, true );
		index1 = Index.getInstance( basename + "1-text", true, true );
		index2 = Index.getInstance( basename + "0-text", true, true );
		index3 = Index.getInstance( basename + "1-text", true, true );
		indexMap = new Object2ReferenceOpenHashMap<String, Index>( new String[] { "index0", "index1", "index2", "index3" }, new Index[] { index0, index1, index2, index3 } );
	}

	@AfterClass
	public static void tearDown() {
		for( File f: new File( basename ).getParentFile().listFiles( (FileFilter)new PrefixFileFilter( new File( basename ).getName() ) ) )	f.delete();
	}

	@Test
	public void testLowLevel() throws IOException {
		final DocumentIterator documentIterator = ConsecutiveDocumentIterator.getInstance( index0.documents( "a" ), new RemappingDocumentIterator( index1.documents( "b" ), Reference2ReferenceMaps.singleton( index0, index1 ) ) );
		assertTrue( documentIterator.mayHaveNext() );
		assertEquals( 0, documentIterator.nextDocument() );
		IntervalIterator intervalIterator = documentIterator.intervalIterator( index0 );
		assertEquals( it.unimi.dsi.util.Interval.valueOf( 0, 1 ), intervalIterator.nextInterval() );
		assertEquals( END_OF_LIST, documentIterator.nextDocument() );
	}

	@Test
	public void testQuery() throws IOException, QueryBuilderVisitorException {
		Query query = new Consecutive( new Term( "a" ), new Remap( new Select( "index1", new Term( "b" ) ), new CharSequence[] { "index1" }, new CharSequence[] { "index0" } ) );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( indexMap, index0, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		assertTrue( documentIterator.mayHaveNext() );
		assertEquals( 0, documentIterator.nextDocument() );
		IntervalIterator intervalIterator = documentIterator.intervalIterator( index0 );
		assertEquals( it.unimi.dsi.util.Interval.valueOf( 0, 1 ), intervalIterator.nextInterval() );
		assertEquals( END_OF_LIST, documentIterator.nextDocument() );
	}

	
	@Test
	public void testDoubleRemapping() throws IOException, QueryBuilderVisitorException {
		Query query = new Remap( new Or( new Term( "a" ), new Select( "index1", new Term( "b" ) ) ), new CharSequence[] { "index0", "index1" }, new CharSequence[] { "index2", "index3" } );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( indexMap, index0, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		IntervalIterator intervalIterator;
		
		assertTrue( documentIterator.mayHaveNext() );

		assertEquals( 0, documentIterator.nextDocument() );
		intervalIterator = documentIterator.intervalIterator( index0 );
		assertSame( IntervalIterators.FALSE, intervalIterator );
		intervalIterator = documentIterator.intervalIterator( index1 );
		assertSame( IntervalIterators.FALSE, intervalIterator );
		intervalIterator = documentIterator.intervalIterator( index2 );
		assertEquals( it.unimi.dsi.util.Interval.valueOf( 0 ), intervalIterator.nextInterval() );
		intervalIterator = documentIterator.intervalIterator( index3 );
		assertEquals( it.unimi.dsi.util.Interval.valueOf( 1 ), intervalIterator.nextInterval() );
		
		assertEquals( 1, documentIterator.nextDocument() );
		intervalIterator = documentIterator.intervalIterator( index0 );
		assertSame( IntervalIterators.FALSE, intervalIterator );
		intervalIterator = documentIterator.intervalIterator( index1 );
		assertSame( IntervalIterators.FALSE, intervalIterator );
		intervalIterator = documentIterator.intervalIterator( index2 );
		assertSame( IntervalIterators.FALSE, intervalIterator );
		intervalIterator = documentIterator.intervalIterator( index3 );
		assertEquals( it.unimi.dsi.util.Interval.valueOf( 0 ), intervalIterator.nextInterval() );
		assertEquals( END_OF_LIST, documentIterator.nextDocument() );
	}
}
