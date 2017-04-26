package it.unimi.di.big.mg4j.tool;

import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.tool.Combine.IndexType;

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

public class MetadataOnlyTest {

	private static String basename;

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		basename = File.createTempFile( MetadataOnlyTest.class.getSimpleName(), "tmp" ).getCanonicalPath();
		new IndexBuilder( basename + "0", new StringArrayDocumentCollection( "a", "c", "a", "d" ) ).run();
		new IndexBuilder( basename + "1", new StringArrayDocumentCollection( "a", "c b", "a b" ) ).run();

		new Paste( IOFactory.FILESYSTEM_FACTORY, basename, new String[] { basename + "0-text", basename + "1-text" }, false, true, 1024, null, 1024, CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX, IndexType.QUASI_SUCCINCT, false, 64, 10, 1024, 1000 ).run();		
		new Paste( IOFactory.FILESYSTEM_FACTORY, basename + "-mo", new String[] { basename + "0-text", basename + "1-text" }, true, true, 1024, null, 1024, CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX, IndexType.QUASI_SUCCINCT, false, 64, 10, 1024, 1000 ).run();
	}

	@AfterClass
	public static void tearDown() {
		for( File f: new File( basename ).getParentFile().listFiles( (FileFilter)new PrefixFileFilter( new File( basename ).getName() ) ) )	f.delete();
	}
	
	@Test
	public void testPaste() throws IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Index index = Index.getInstance( basename );
		assertEquals( 2, index.documents( 0 ).frequency() );			
		assertEquals( 2, index.documents( 1 ).frequency() );
		assertEquals( 1, index.documents( 2 ).frequency() );
		assertEquals( 1, index.documents( 3 ).frequency() );
	}
}
