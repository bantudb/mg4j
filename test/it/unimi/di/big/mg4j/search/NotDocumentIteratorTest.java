package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

public class NotDocumentIteratorTest {

	@Test
	public void testNot() throws IOException {
		DocumentIterator i = new IntArrayDocumentIterator( new long[] { 2, 4, 7 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } },
				{ { 1, 2 } },
				{ {} },
				} );
		
		NotDocumentIterator notDocumentIterator = NotDocumentIterator.getInstance( i, 8 );
		assertEquals( 0, notDocumentIterator.nextDocument() );
		assertEquals( 0, notDocumentIterator.document() );
		assertEquals( IntervalIterators.TRUE, notDocumentIterator.intervalIterator() );
		assertEquals( 1, notDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, notDocumentIterator.intervalIterator() );
		assertEquals( 3, notDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, notDocumentIterator.intervalIterator() );
		assertEquals( 5, notDocumentIterator.nextDocument() );
		assertEquals( 5, notDocumentIterator.document() );
		assertEquals( IntervalIterators.TRUE, notDocumentIterator.intervalIterator() );
		assertEquals( 6, notDocumentIterator.nextDocument() );
		assertEquals( IntervalIterators.TRUE, notDocumentIterator.intervalIterator() );
		assertEquals( END_OF_LIST, notDocumentIterator.nextDocument() );
		assertEquals( END_OF_LIST, notDocumentIterator.nextDocument() );
		notDocumentIterator.dispose();
	}
	
	@Test
	public void testSkip() throws IOException {
		DocumentIterator i = new IntArrayDocumentIterator( new long[] { 2, 4, 7 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } },
				{ { 1, 2 } },
				{ {} },
				} );
		
		NotDocumentIterator notDocumentIterator = NotDocumentIterator.getInstance( i, 9 );
	
		assertEquals( 3, notDocumentIterator.skipTo( 3 ) );
		assertEquals( IntervalIterators.TRUE, notDocumentIterator.intervalIterator() );
		assertEquals( 3, notDocumentIterator.skipTo( 2 ) );
		assertEquals( 8, notDocumentIterator.skipTo( 8 ) );
		assertEquals( DocumentIterator.END_OF_LIST, notDocumentIterator.skipTo( 9 ) );
		assertEquals( END_OF_LIST, notDocumentIterator.nextDocument() );
		assertEquals( END_OF_LIST, notDocumentIterator.nextDocument() );
	}
	
	@Test
	public void testSkipAtStart() throws IOException {
		DocumentIterator i = new IntArrayDocumentIterator( new long[] { 0, 3 }, 
				new int[][][] { 
				{ { 0 } },
				{ { 0 } },
				} );
		
		NotDocumentIterator notDocumentIterator = NotDocumentIterator.getInstance( i, 9 );
		assertEquals( 1, notDocumentIterator.skipTo( 0 ) );
	}
	
}
