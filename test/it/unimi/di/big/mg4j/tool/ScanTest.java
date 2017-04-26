package it.unimi.di.big.mg4j.tool;

import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexIterators;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

public class ScanTest {

	@Test
	public void testEverywhereTerms() throws IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		
		String basename = File.createTempFile( getClass().getSimpleName(), "everywhereTerms" ).getCanonicalPath();
		new IndexBuilder( basename, new StringArrayDocumentCollection( "a a" ) ).keepBatches( true ).run();
		IndexIterator indexIterator = Index.getInstance( basename + "-text@0" ).documents( 0 );
		indexIterator.nextDocument();
		assertEquals( 2, indexIterator.count() );
		int[] position = IndexIterators.positionArray( indexIterator );
		assertEquals( 0, position[ 0 ] );
		assertEquals( 1, position[ 1 ] );
	} 


}
