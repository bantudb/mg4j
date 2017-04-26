package it.unimi.di.big.mg4j.index;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.DateArrayDocumentCollection;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.tool.IndexBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.sql.Date;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BitStreamIndexReaderTest {

	private static String basename0, basename1, basename2, basename3, basename4;
	private static Index index0, index1, index2, index3, index4;

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		basename0 = File.createTempFile( BitStreamIndexReaderTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename0, new StringArrayDocumentCollection( new String[] { "a", "a", "c" } ) ).run();
		index0 = Index.getInstance( basename0 + "-text", true, false );

		basename1 = File.createTempFile( BitStreamIndexReaderTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename1, new StringArrayDocumentCollection( "a", "a", "a", "b" ) ).quantum( 2 ).height( 0 ).run();
		index1 = Index.getInstance( basename1 + "-text", true, false );

		basename2 = File.createTempFile( BitStreamIndexReaderTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename2, new StringArrayDocumentCollection( "a", "a", "a", "b" ) ).interleaved( false ).skips( false ).run();
		index2 = Index.getInstance( basename2 + "-text", true, false );

		basename3 = File.createTempFile( BitStreamIndexReaderTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename3, new StringArrayDocumentCollection( "a", "a", "a", "b" ) ).interleaved( false ).skips( true ).run();
		index3 = Index.getInstance( basename3 + "-text", true, false );

		basename4 = File.createTempFile( BitStreamIndexReaderTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename4, new DateArrayDocumentCollection( new Date( 0 ) ) ).skips( false ).run();
		index4 = Index.getInstance( basename4 + "-date", true, false );
	}

	@AfterClass
	public static void tearDown() {
		for ( Object f : FileUtils.listFiles( new File( basename0 ).getParentFile(), FileFilterUtils.prefixFileFilter( BitStreamIndexReaderTest.class.getSimpleName() ), null ) )
			( (File)f ).delete();
	}

	
	@Test
	public void testSkipToEndOfList() throws IOException {
		IndexIterator indexIterator = index0.documents( "a" );
		assertEquals( DocumentIterator.END_OF_LIST, indexIterator.skipTo( DocumentIterator.END_OF_LIST ) );
		assertEquals( END_OF_LIST, indexIterator.document() );
		indexIterator.dispose();

		indexIterator = index0.documents( "a" );
		assertEquals( 0, indexIterator.skipTo( 0 ) );
		assertEquals( 1, indexIterator.skipTo( 1 ) );
		assertEquals( DocumentIterator.END_OF_LIST, indexIterator.skipTo( 2 ) );
		assertEquals( END_OF_LIST, indexIterator.document() );
		indexIterator.dispose();
	}

	@Test
	public void testSkipBackwardsAtEndOfList() throws IOException {
		IndexIterator documents = index1.documents( 0 );
		assertEquals( 0, documents.nextDocument() );
		assertEquals( 1, documents.nextDocument() );
		assertEquals( DocumentIterator.END_OF_LIST, documents.skipTo( 3 ) );
		assertEquals( DocumentIterator.END_OF_LIST, documents.skipTo( 0 ) );
		assertEquals( DocumentIterator.END_OF_LIST, documents.document() );
	
		documents = index2.documents( 0 );
		assertEquals( 0, documents.nextDocument() );
		assertEquals( 1, documents.nextDocument() );
		assertEquals( DocumentIterator.END_OF_LIST, documents.skipTo( 3 ) );
		assertEquals( DocumentIterator.END_OF_LIST, documents.skipTo( 0 ) );
		assertEquals( DocumentIterator.END_OF_LIST, documents.document() );

		documents = index3.documents( 0 );
		assertEquals( 0, documents.nextDocument() );
		assertEquals( 1, documents.nextDocument() );
		assertEquals( DocumentIterator.END_OF_LIST, documents.skipTo( 3 ) );
		assertEquals( DocumentIterator.END_OF_LIST, documents.skipTo( 0 ) );
		assertEquals( DocumentIterator.END_OF_LIST, documents.document() );
	}

	@Test
	public void testEndOfPayloadList() throws IOException {
		IndexIterator indexIterator = index4.documents( 0 );
		assertEquals( 0, indexIterator.nextDocument() );
		assertEquals( DocumentIterator.END_OF_LIST, indexIterator.nextDocument() );
		indexIterator.dispose();
	}
}
