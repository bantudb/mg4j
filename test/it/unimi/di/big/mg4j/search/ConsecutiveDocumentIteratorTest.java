package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

import org.junit.Test;

public class ConsecutiveDocumentIteratorTest {

	@Test
	public void testTrue() throws IOException {
		IntArrayDocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } }, 
				{ {} },
				{ { 0 } },
				} );
		IntArrayDocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{ {} }, 
				{ { 2 } }, 
				{ { 1 } },
				} );
		IntArrayDocumentIterator i2 = new IntArrayDocumentIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][][] { 
				{ {} }, 
				{ { 2 }, { 5 } }, 
				{ {} }, 
				{ { 2 } },
				} );
		DocumentIterator consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( i0, i1, i2 );
		assertTrue( consecutiveDocumentIterator.mayHaveNext() );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 0, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 1 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 1, 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 1, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 1, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 2, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 2, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 3, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 3, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );
		
		i0.reset();
		i1.reset();
		i2.reset();

		consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( i0, i1, i2 );
		assertEquals( 2, consecutiveDocumentIterator.skipTo( 2 ) );
		assertEquals( 2, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 3, consecutiveDocumentIterator.nextDocument() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );

		i0.reset();
		i1.reset();
		i2.reset();

		consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( new DocumentIterator[] { i0, i1, i2 }, new int[] { 1, 1, 1 } );
		assertTrue( consecutiveDocumentIterator.mayHaveNext() );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 0, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 1, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 1, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 5 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 2, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 2, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );
	}

	@Test
	public void testIntervalIterator() throws IOException {
		IntArrayDocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				} );
		IntArrayDocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 2, 3 } }, 
				} );
		IntArrayDocumentIterator i2 = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 4, 5 } }, 
				} );
		DocumentIterator consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( i0, i1, i2 );
		assertTrue( consecutiveDocumentIterator.mayHaveNext() );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 0, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 5 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );
	}

	@Test
	public void testMultipleAlignments() throws IOException {
		IntArrayDocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				} );
		IntArrayDocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0 }, 
				new int[][][] { 
				{ { 3, 4 } }, 
				} );
		DocumentIterator consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( i0, i1 );
		assertTrue( consecutiveDocumentIterator.mayHaveNext() );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( Interval.valueOf( 1, 4 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );
	}
	
	@Test
	public void testIndexIntervalIterator() throws IOException {
		IntArrayIndexIterator i0 = new IntArrayIndexIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][] { 
				{ 0, 3 }, 
				{ 2 }, 
				{ 0 }, 
				{ 0, 1, 2, 3, 4, 5, 6 }, 
				} );
		IntArrayIndexIterator i1 = new IntArrayIndexIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][] { 
				{ 1, 4 }, 
				{ 1 }, 
				{ 1 }, 
				{ 1, 5, 8 }, 
				} );
		IntArrayIndexIterator i2 = new IntArrayIndexIterator( new long[] { 0, 1, 2, 3 }, 
				new int[][] { 
				{ 2, 5 }, 
				{ 0 }, 
				{ 2 }, 
				{ 4, 6 }, 
				} );
		DocumentIterator consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( i0, i1, i2 );
		assertTrue( consecutiveDocumentIterator.mayHaveNext() );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 0, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( Interval.valueOf( 3, 5 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 2, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 2, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 3, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 3, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 4, 6 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );

		
		i0.reset();
		i1.reset();
		i2.reset();

		consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( i0, i1, i2 );
		assertEquals( 2, consecutiveDocumentIterator.skipTo( 1 ) );
		assertEquals( 2, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 0, 2 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 3, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 3, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 4, 6 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );

		i0.reset();
		i1.reset();
		i2.reset();

		consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( new DocumentIterator[] { i0, i1, i2 }, new int[] { 1, 0, 0 } );
		assertTrue( consecutiveDocumentIterator.mayHaveNext() );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 0, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 2, 5 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( 3, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 3, consecutiveDocumentIterator.document() );
		assertEquals( Interval.valueOf( 3, 6 ), consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertNull( consecutiveDocumentIterator.intervalIterator().nextInterval() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );
	}
	
	@Test
	public void testExtentDocumentIterator() throws IOException {
		IntArrayDocumentIterator i0 = new IntArrayDocumentIterator( new long[] { 0, 1, 2 }, 
				new int[][][] { 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 0, 1 }, { 1, 2 } }, 
				{ { 1 }, { 2 } }, 
				} );
		IntArrayDocumentIterator i1 = new IntArrayDocumentIterator( new long[] { 0, 1, 2 }, 
				new int[][][] { 
				{ { 5, 7 } }, 
				{ {} }, 
				{ { 2 }, { 3 }, { 4 } }, 
				} );
		
		DocumentIterator consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( new DocumentIterator[] { i0, i1 }, new int[] { 1, 2 } );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 8, consecutiveDocumentIterator.intervalIterator().extent() );
		assertEquals( 1, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 3, consecutiveDocumentIterator.intervalIterator().extent() );
		assertEquals( 2, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 5, consecutiveDocumentIterator.intervalIterator().extent() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );
	}
	
	@Test
	public void testExtentIndexIterator() throws IOException {
		IntArrayIndexIterator i0 = new IntArrayIndexIterator( new long[] { 0 }, 
				new int[][] { 
				{ 0, 3 }, 
				} );
		IntArrayIndexIterator i1 = new IntArrayIndexIterator( new long[] { 0 }, 
				new int[][] { 
				{ 1, 4, 6 }, 
				} );

		DocumentIterator consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( new IndexIterator[] { i0, i1 }, new int[] { 1, 2 } );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 5, consecutiveDocumentIterator.intervalIterator().extent() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );

		i0.reset();
		i1.reset();
		consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( new IndexIterator[] { i0, i1 }, new int[] { 0, 0 } );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 2, consecutiveDocumentIterator.intervalIterator().extent() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );

		i0.reset();
		i1.reset();
		consecutiveDocumentIterator = ConsecutiveDocumentIterator.getInstance( new IndexIterator[] { i0, i1 }, new int[] { 1, 0 } );
		assertEquals( 0, consecutiveDocumentIterator.nextDocument() );
		assertEquals( 3, consecutiveDocumentIterator.intervalIterator().extent() );
		assertEquals( END_OF_LIST, consecutiveDocumentIterator.nextDocument() );
	}
}
