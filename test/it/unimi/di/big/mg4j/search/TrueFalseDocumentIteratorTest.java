package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
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

public class TrueFalseDocumentIteratorTest {
	private static Index index;
	private static String basename;

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		basename = File.createTempFile( TrueFalseDocumentIteratorTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename, new StringArrayDocumentCollection( "a", "b", "c" ) ).run();
		index = Index.getInstance( basename + "-text", true, true );
	}

	@AfterClass
	public static void tearDown() {
		for( File f: new File( basename ).getParentFile().listFiles( (FileFilter)new PrefixFileFilter( new File( basename ).getName() ) ) )	f.delete();
	}
	
	@Test
	public void testTrue() {
		TrueDocumentIterator trueDocumentIterator = TrueDocumentIterator.getInstance( index );
		assertEquals( 0, trueDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, trueDocumentIterator.intervalIterator() );
		assertEquals( 1, trueDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, trueDocumentIterator.intervalIterator() );
		assertEquals( 2, trueDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, trueDocumentIterator.intervalIterator() );
		assertEquals( END_OF_LIST, trueDocumentIterator.nextDocument() );
		assertEquals( END_OF_LIST, trueDocumentIterator.nextDocument() );
	}

	@Test
	public void testFalse() {
		FalseDocumentIterator falseDocumentIterator = FalseDocumentIterator.getInstance( index );
		assertEquals( END_OF_LIST, falseDocumentIterator.nextDocument() );
		assertEquals( END_OF_LIST, falseDocumentIterator.nextDocument() );
	}

}
