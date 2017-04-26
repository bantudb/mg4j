package it.unimi.di.big.mg4j.search;

import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.tool.IndexBuilder;

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

public class LowPassDocumentIteratorTest {
	private static Index index;
	private static SimpleParser simpleParser;
	private static String basename;

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		basename = File.createTempFile( LowPassDocumentIteratorTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename, new StringArrayDocumentCollection( "a", "a b c d", "c" ) ).run();
		index = Index.getInstance( basename + "-text", true, true );
		simpleParser = new SimpleParser( index.termProcessor );
	}

	@AfterClass
	public static void tearDown() {
		for( File f: new File( basename ).getParentFile().listFiles( (FileFilter)new PrefixFileFilter( new File( basename ).getName() ) ) )	f.delete();
	}

	@Test
	public void testSkipBug() throws QueryParserException, QueryBuilderVisitorException, IOException {
		Query query = simpleParser.parse( "(a < b)~5 c" );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, index, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		assertEquals( 1, documentIterator.nextDocument() );
		documentIterator.dispose();
	}
	
	@Test
	public void testSkipBug2() throws QueryParserException, QueryBuilderVisitorException, IOException {
		Query query = simpleParser.parse( "((a < b)~5 < c) d" );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, index, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		assertEquals( 1, documentIterator.nextDocument() );
		documentIterator.dispose();
	}
}
