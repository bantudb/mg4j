package it.unimi.di.big.mg4j.mock.search;

import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.And;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.nodes.Term;
import it.unimi.di.big.mg4j.query.nodes.Weight;
import it.unimi.di.big.mg4j.search.DocumentIterator;
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


public class WeightTest {
	private static Index index;
	private static String basename;

	@BeforeClass
	public static void setUp() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		basename = File.createTempFile( WeightTest.class.getSimpleName(), "test" ).getCanonicalPath();
		new IndexBuilder( basename, new StringArrayDocumentCollection( "a", "b", "c" ) ).run();
		index = Index.getInstance( basename + "-text", true, true );
	}

	@AfterClass
	public static void tearDown() {
		for( File f: new File( basename ).getParentFile().listFiles( (FileFilter)new PrefixFileFilter( new File( basename ).getName() ) ) )	f.delete();
	}

	@Test
	public void testWeights() throws QueryBuilderVisitorException, IOException {
		Query query = new Weight( 0.5, new And( new Term( "a" ), new Term( "b" ) ) );
		DocumentIteratorBuilderVisitor documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, index, Integer.MAX_VALUE );
		DocumentIterator documentIterator = query.accept( documentIteratorBuilderVisitor );
		assertEquals( .5, documentIterator.weight(), 0 );
		documentIterator.dispose();

		query = new Weight( .1, new Weight( 0.5, new And( new Weight( .2, new Term( "a" ) ), new Term( "b" ) ) ) );
		documentIteratorBuilderVisitor = new DocumentIteratorBuilderVisitor( null, index, Integer.MAX_VALUE );
		documentIterator = query.accept( documentIteratorBuilderVisitor );
		assertEquals( .5, documentIterator.weight(), 0 );
		documentIterator.dispose();
	}
	
}
