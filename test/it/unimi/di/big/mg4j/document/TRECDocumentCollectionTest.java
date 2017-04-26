package it.unimi.di.big.mg4j.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Reader;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class TRECDocumentCollectionTest {

	@Test
	public void testChar255() throws Exception {
		File temp = File.createTempFile(TRECDocumentCollectionTest.class
				.getName(), ".testChar255");

		temp.deleteOnExit();
		OutputStream outputStream = new FileOutputStream(temp);
		IOUtils.copy(this.getClass().getResourceAsStream("testChar255.data"),
				outputStream);
		outputStream.close();

		TRECDocumentCollection collection = new TRECDocumentCollection(
				new String[] { temp.toString() },
				CompositeDocumentFactory
						.getFactory(new DocumentFactory[] {
								new TRECHeaderDocumentFactory(),
								new HtmlDocumentFactory( new String[] { "encoding=ISO-8859-1" } ) } ),
				4, // Very small, to induce fragmentation
				false);

		try {
			DocumentIterator iter = collection.iterator();
			Document d;
			while ((d = iter.nextDocument()) != null)
				d.title();
		} catch (IllegalStateException e) {
			assertTrue(false);
		}
		
		collection.close();

	}

	@Test
	public void testContents() throws Exception {
		File temp = File.createTempFile( TRECDocumentCollectionTest.class.getName(), ".testContents" );
		File tempAgain = File.createTempFile( TRECDocumentCollectionTest.class.getName(), ".testContentsAgain" );

		temp.deleteOnExit();
		tempAgain.deleteOnExit();
		OutputStream outputStream = new FileOutputStream( temp );
		OutputStream outputStreamAgain = new FileOutputStream( tempAgain );
		IOUtils.copy( this.getClass().getResourceAsStream( "testContents.data" ), outputStream );
		outputStream.close();
		IOUtils.copy( this.getClass().getResourceAsStream( "testContentsAgain.data" ), outputStreamAgain );
		outputStreamAgain.close();

		TRECDocumentCollection collection = new TRECDocumentCollection(
				new String[] { temp.toString(), tempAgain.toString() },
				CompositeDocumentFactory
						.getFactory(new DocumentFactory[] {
								new TRECHeaderDocumentFactory(),
								new HtmlDocumentFactory( new String[] { "encoding=ISO-8859-1" } ) } ),
				4, // Very small, to induce fragmentation
				false);

		DocumentIterator iter = collection.iterator();
		Document d = null;

		d = iter.nextDocument();
		assertNotNull(d);
		assertEquals("http://gx0001/", d.uri());
		assertEquals("GX001", d.title());

		final int textIndex = collection.factory().fieldIndex( "text" );
		
		assertEquals( "Line 1\n     The line 2!\n  Mamma\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );
		
		d = iter.nextDocument();
		assertNotNull(d);
		assertEquals("http://gx0002/", d.uri());
		assertEquals("GX002", d.title());

		assertEquals( "Contents of this file reside on one line only\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );

		d = iter.nextDocument();
		assertNotNull(d);
		assertEquals("http://gx0003/", d.uri());
		assertEquals("GX003", d.title());

		assertEquals( "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );

		d = iter.nextDocument();
		assertNotNull(d);
		assertEquals("http://gx0004/", d.uri());
		assertEquals("GX004", d.title());

		assertEquals( "New content 0\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );
		
		d = iter.nextDocument();
		assertNotNull(d);
		assertEquals("http://gx0005/", d.uri());
		assertEquals("GX005", d.title());

		assertEquals( "New content 1\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );

		d = iter.nextDocument();
		assertNotNull(d);
		assertEquals("http://gx0006/", d.uri());
		assertEquals("GX006", d.title());

		assertEquals( "New content 2\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );

		d = iter.nextDocument();
		assertNotNull(d);
		assertEquals("http://gx0007/", d.uri());
		assertEquals("GX007", d.title());

		assertEquals( "", IOUtils.toString( (Reader)d.content( textIndex ) ) );

		d = iter.nextDocument();
		assertNull(d);
		iter.close();
		
		d = collection.document( 0 );
		assertNotNull(d);
		assertEquals("http://gx0001/", d.uri());
		assertEquals("GX001", d.title());

		assertEquals( "Line 1\n     The line 2!\n  Mamma\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );
		d.close();
		
		d = collection.document( 1 );
		assertNotNull(d);
		assertEquals("http://gx0002/", d.uri());
		assertEquals("GX002", d.title());

		assertEquals( "Contents of this file reside on one line only\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );
		d.close();

		d = collection.document( 2 );
		assertNotNull(d);
		assertEquals("http://gx0003/", d.uri());
		assertEquals("GX003", d.title());

		assertEquals( "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );
		d.close();

		d = collection.document( 3 );
		assertNotNull(d);
		assertEquals("http://gx0004/", d.uri());
		assertEquals("GX004", d.title());

		assertEquals( "New content 0\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );
		d.close();
		
		d = collection.document( 4 );
		assertNotNull(d);
		assertEquals("http://gx0005/", d.uri());
		assertEquals("GX005", d.title());

		assertEquals( "New content 1\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );
		d.close();

		d = collection.document( 5 );
		assertNotNull(d);
		assertEquals("http://gx0006/", d.uri());
		assertEquals("GX006", d.title());

		assertEquals( "New content 2\n", IOUtils.toString( (Reader)d.content( textIndex ) ) );
		d.close();

		d = collection.document( 6 );
		assertNotNull(d);
		assertEquals("http://gx0007/", d.uri());
		assertEquals("GX007", d.title());

		assertEquals( "", IOUtils.toString( (Reader)d.content( textIndex ) ) );
		d.close();

		collection.close();
	}
}
