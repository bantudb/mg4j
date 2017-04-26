package it.unimi.di.big.mg4j.index;

import static it.unimi.di.big.mg4j.tool.Combine.IndexType.HIGH_PERFORMANCE;
import static it.unimi.di.big.mg4j.tool.Combine.IndexType.INTERLEAVED;
import static it.unimi.di.big.mg4j.tool.Combine.IndexType.QUASI_SUCCINCT;
import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.AbstractDocument;
import it.unimi.di.big.mg4j.document.AbstractDocumentSequence;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.document.IdentityDocumentFactory;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.tool.Combine.IndexType;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IndexSlowTest {

	private static String basename;

	private final static class VerticalDocumentSequence extends AbstractDocumentSequence {
		private final static long NUMBER_OF_DOCUMENTS = ( 1L << 31 ) + 1000000;
		
		@Override
		public DocumentIterator iterator() throws IOException {
			return new DocumentIterator() {
				long i = -1;
				WordReader wordReader = new FastBufferedReader();
				Document document = new AbstractDocument() {
					@Override
					public WordReader wordReader( int field ) {
						return wordReader;
					}
					
					@Override
					public CharSequence uri() {
						return null;
					}
					
					@Override
					public CharSequence title() {
						return null;
					}
					
					@Override
					public Object content( int field ) throws IOException {
						return new StringReader( ( i & -i ) == i ? "0 1" : i % 10 == 9 ? "0 2" : "0" );
					}
				};
				@Override
				public Document nextDocument() throws IOException {
					if ( i == NUMBER_OF_DOCUMENTS - 1 ) return null;
					i++;
					return document;
				}

				@Override
				public void close() throws IOException {}
			};
		
		}

		@Override
		public DocumentFactory factory() {
			return new IdentityDocumentFactory();
		}
		
	};


	private final static class HorizontalDocumentSequence extends AbstractDocumentSequence {
		private final static long TARGET_NUMBER_OF_TERMS = ( 1L << 31 ) + 1000000;
		private final static long TERMS_PER_DOCUMENT = 10000;
		private static final long NUMBER_OF_DOCUMENTS = ( TARGET_NUMBER_OF_TERMS + TERMS_PER_DOCUMENT - 1 ) / TERMS_PER_DOCUMENT;
		
		@Override
		public DocumentIterator iterator() throws IOException {
			return new DocumentIterator() {
				long i = -1;
				WordReader wordReader = new FastBufferedReader();
				Document document = new AbstractDocument() {
					@Override
					public WordReader wordReader( int field ) {
						return wordReader;
					}
					
					@Override
					public CharSequence uri() {
						return null;
					}
					
					@Override
					public CharSequence title() {
						return null;
					}
					
					@Override
					public Object content( int field ) throws IOException {
						MutableString s = new MutableString(), d = new MutableString();
						for( int j = 0; j < TERMS_PER_DOCUMENT; j++ ) {
							d.setLength( 0 ).append( "0000000000" ).append( i * TERMS_PER_DOCUMENT + j );
							s.append( d.subSequence( d.length() - 10, d.length() ) ).append( ' ' ); 
						}
						return new FastBufferedReader( s );
					}
				};
				@Override
				public Document nextDocument() throws IOException {
					if ( i == NUMBER_OF_DOCUMENTS - 1 ) return null;
					i++;
					return document;
				}

				@Override
				public void close() throws IOException {}
			};
		
		}

		@Override
		public DocumentFactory factory() {
			return new IdentityDocumentFactory();
		}
		
	};

	@BeforeClass
	public static void setUp() throws IOException {
		basename = File.createTempFile( IndexSlowTest.class.getSimpleName(), "test" ).getCanonicalPath();
	}

	@AfterClass
	public static void tearDown() throws IOException {
		for ( Object f : FileUtils.listFiles( new File( basename ).getParentFile(), FileFilterUtils.prefixFileFilter( IndexSlowTest.class.getSimpleName() ), null ) )
			( (File)f ).delete();
		if ( lastSequence != null ) lastSequence.close();
	}

	// We keep track of the last returned sequence to close it without cluttering the test code
	private static DocumentSequence lastSequence;

	public void testIndex( IndexType indexType, Map<Component, Coding> flags, int quantum, int height, TermProcessor termProcessor ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException,
			InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Index index;
		
		// Vanilla indexing
		new IndexBuilder( basename, new VerticalDocumentSequence() ).standardWriterFlags( flags ).termProcessor( termProcessor ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum )
				.height( height ).documentsPerBatch( 100000000 ).termMapClass( null ).run();
		index = Index.getInstance( basename + "-text?mapped=1" );
		assertEquals( VerticalDocumentSequence.NUMBER_OF_DOCUMENTS, index.numberOfDocuments );
		assertEquals( 3, index.numberOfTerms );
		IndexIterator documents0 = index.documents( 0 );
		IndexIterator documents1 = index.documents( 1 );
		IndexIterator documents2 = index.documents( 2 );
		for( long i = 0; i < VerticalDocumentSequence.NUMBER_OF_DOCUMENTS; i++ ) {
			assertEquals( i, documents0.nextDocument() );
			if ( index.hasCounts ) assertEquals( 1, documents0.count() );
			if ( index.hasPositions ) assertEquals( 0, documents0.nextPosition() );
			
			if ( ( i & -i ) == i ) {
				assertEquals( i, documents1.nextDocument() );
				if ( index.hasCounts ) assertEquals( 1, documents1.count() );
				if ( index.hasPositions ) assertEquals( 1, documents1.nextPosition() );
			}
			else if ( i % 10 == 9 ) {
				assertEquals( i, documents2.nextDocument() );
				if ( index.hasCounts ) assertEquals( 1, documents2.count() );
				if ( index.hasPositions ) assertEquals( 1, documents2.nextPosition() );
			}
		}

		documents0.dispose();
		documents1.dispose();

		new IndexBuilder( basename, new HorizontalDocumentSequence() ).standardWriterFlags( flags ).termProcessor( termProcessor ).skipBufferSize( 1024 ).pasteBufferSize( 1024 ).indexType( indexType ).skips( quantum != 0 ).quantum( quantum )
		.height( height ).documentsPerBatch( 100000000 ).termMapClass( null ).run();

		index = Index.getInstance( basename + "-text?mapped=1" );

		assertEquals( HorizontalDocumentSequence.NUMBER_OF_DOCUMENTS, index.numberOfDocuments );
		assertEquals( HorizontalDocumentSequence.NUMBER_OF_DOCUMENTS * HorizontalDocumentSequence.TERMS_PER_DOCUMENT, index.numberOfTerms );

		for( long i = 0; i < HorizontalDocumentSequence.TARGET_NUMBER_OF_TERMS; i++ ) {
			IndexIterator documents = index.documents( i );
			assertEquals( 1, documents.frequency() );
			assertEquals( i / HorizontalDocumentSequence.TERMS_PER_DOCUMENT, documents.nextDocument() );
			if ( index.hasCounts ) assertEquals( 1, documents.count() );
			if ( index.hasPositions ) assertEquals( i % HorizontalDocumentSequence.TERMS_PER_DOCUMENT, documents.nextPosition() );
			documents.dispose();
		}
	}

	public void testIndex( IndexType indexType, int quantum, int height ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException,
			IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		testIndex( indexType, indexType == QUASI_SUCCINCT ? CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX : CompressionFlags.DEFAULT_STANDARD_INDEX, quantum, height, DowncaseTermProcessor.getInstance() );
	}

	public void testIndex( IndexType indexType, Map<Component, Coding> flags, int quantum, int height ) throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		testIndex( indexType, flags, quantum, height, DowncaseTermProcessor.getInstance() );
	}
	
	@Test
	public void testIndex() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {

		Reference2ObjectOpenHashMap<Component, Coding> flags = new Reference2ObjectOpenHashMap<Component, Coding>( CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX );
		flags.remove( Component.POSITIONS );
		testIndex( QUASI_SUCCINCT, flags, 256, 0 );
		testIndex( QUASI_SUCCINCT, 256, 0 );

		flags = new Reference2ObjectOpenHashMap<Component, Coding>( CompressionFlags.DEFAULT_STANDARD_INDEX );
		flags.remove( Component.POSITIONS );
		testIndex( INTERLEAVED, flags, 256, 16 );
		testIndex( INTERLEAVED, flags, -1, 16 );
		flags.remove( Component.COUNTS );

		testIndex( INTERLEAVED, flags, 256, 16 );
		testIndex( INTERLEAVED, flags, -1, 16 );
		
		testIndex( HIGH_PERFORMANCE, 256, 16 );
		testIndex( HIGH_PERFORMANCE, -1, 16 );

	}
}
