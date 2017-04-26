package it.unimi.di.big.mg4j.index;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.tool.IndexBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BitStreamHPIndexReaderTest {
	private static Index index0, index1, index2, index3;
	private static String basename0, basename1, basename2, basename3;
	
	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		basename0 = File.createTempFile( BitStreamHPIndexReaderTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename0, new StringArrayDocumentCollection( "a", "a", "a", "b" ) ).quantum( 2 ).run();
		index0 = Index.getInstance( basename0 + "-text", true, false );

		basename1 = File.createTempFile( BitStreamHPIndexReaderTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename1, new StringArrayDocumentCollection( "a", "a", "b" ) ).quantum( 1 ).run();
		index1 = Index.getInstance( basename1 + "-text", true, false );

		basename2 = File.createTempFile( BitStreamHPIndexReaderTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename2, new StringArrayDocumentCollection( "a", "a", "a", "b" ) ).interleaved( true ).quantum( 2 ).run();
		index2 = Index.getInstance( basename2 + "-text", true, false );

		basename3 = File.createTempFile( BitStreamHPIndexReaderTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename3, new StringArrayDocumentCollection( "a", "a", "b" ) ).interleaved( true ).quantum( 1 ).run();
		index3 = Index.getInstance( basename3 + "-text", true, false );
	}

	@AfterClass
	public static void tearDown() {
		for ( Object f : FileUtils.listFiles( new File( basename0 ).getParentFile(), FileFilterUtils.prefixFileFilter( BitStreamHPIndexReader.class.getSimpleName() ), null ) )
			( (File)f ).delete();
	}

	@Test
	public void testSkipToEndOfListBug() throws IOException {
		testSkipToEndOfListBug( index0 );
		testSkipToEndOfListBug( index1 );
		testSkipToEndOfListBug( index2 );
		testSkipToEndOfListBug( index3 );
	}

	public void testSkipToEndOfListBug( Index index ) throws IOException {
		IndexIterator documents = index.documents( 0 );
		assertEquals( 0, documents.nextDocument() );
		assertEquals( END_OF_LIST, documents.skipTo( END_OF_LIST ) );

		documents = index.documents( 0 );
		assertEquals( 0, documents.nextDocument() );
		assertEquals( END_OF_LIST, documents.skipTo( END_OF_LIST ) );

		documents = index.documents( 0 );
		assertEquals( 0, documents.skipTo( 0 ) );
		assertEquals( END_OF_LIST, documents.skipTo( END_OF_LIST ) );
	}
}
