package it.unimi.di.big.mg4j.io;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.SimpleCompressedDocumentCollectionBuilder;
import it.unimi.di.big.mg4j.document.TestDocumentCollection;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.di.big.mg4j.tool.IndexTest;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.UUID;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.junit.Test;

public class IOFactoryTest {

	private final static class MockIOFactory implements IOFactory {
		private final File prefix;
		
		public MockIOFactory( File prefix ) {
			this.prefix = prefix;
		}

		@Override
		public long length( String name ) throws IOException {
			return FILESYSTEM_FACTORY.length( new File( prefix, name ).getCanonicalPath() );
		}
		
		@Override
		public WritableByteChannel getWritableByteChannel( String name ) throws IOException {
			return FILESYSTEM_FACTORY.getWritableByteChannel( new File( prefix, name ).getCanonicalPath() );
		}
		
		@Override
		public ReadableByteChannel getReadableByteChannel( String name ) throws IOException {
			return FILESYSTEM_FACTORY.getReadableByteChannel( new File( prefix, name ).getCanonicalPath() );
		}
		
		@Override
		public OutputStream getOutputStream( String name ) throws IOException {
			return FILESYSTEM_FACTORY.getOutputStream( new File( prefix, name ).getCanonicalPath() );
		}
		
		@Override
		public InputStream getInputStream( String name ) throws IOException {
			return FILESYSTEM_FACTORY.getInputStream( new File( prefix, name ).getCanonicalPath() );
		}
		
		@Override
		public boolean exists( String name ) throws IOException {
			return FILESYSTEM_FACTORY.exists( new File( prefix, name ).getCanonicalPath() );
		}
		
		@Override
		public boolean delete( String name ) throws IOException {
			return FILESYSTEM_FACTORY.delete( new File( prefix, name ).getCanonicalPath() );
		}
		
		@Override
		public void createNewFile( String name ) throws IOException {
			FILESYSTEM_FACTORY.createNewFile( new File( prefix, name ).getCanonicalPath() );
		}
	};
	
	@Test
	public void test() throws IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		final File basename = File.createTempFile( IOFactoryTest.class.getSimpleName(), "test" );
		assertTrue( basename.delete() );
		assertTrue( basename.mkdir() );
		final MockIOFactory mockIOFactory = new MockIOFactory( basename );
		final TestDocumentCollection testDocumentCollection = new TestDocumentCollection();
		final String indexName = UUID.randomUUID().toString();
		new IndexBuilder( indexName, testDocumentCollection ).ioFactory( mockIOFactory ).
		builder( new SimpleCompressedDocumentCollectionBuilder( mockIOFactory, "testcoll", testDocumentCollection.factory().copy(), true ) ).run();
		IndexTest.checkAgainstContent( testDocumentCollection, null, null, 0, new File( basename, indexName ).getCanonicalPath() );
		final String collectionName = new File( basename, "testcoll.collection" ).toString();
		final DocumentCollection collection = (DocumentCollection)BinIO.loadObject( collectionName );
		collection.filename( collectionName );
		IndexTest.checkAgainstContent( collection, null, null, 0, new File( basename, indexName ).getCanonicalPath() );
		FileUtils.deleteDirectory( basename );
		for( File f : FileUtils.listFiles( new File( System.getProperty("user.dir") ), new PrefixFileFilter( indexName ), null ) ) fail( f.toString() );
	}
}
