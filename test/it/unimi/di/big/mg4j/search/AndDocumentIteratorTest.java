package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

import org.junit.Test;

public class AndDocumentIteratorTest {

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
		
		DocumentIterator andDocumentIterator = AndDocumentIterator.getInstance( i0, i1 );
		assertEquals( 0, andDocumentIterator.nextDocument() );
		assertEquals( 5, andDocumentIterator.intervalIterator().extent() );
		assertEquals( 1, andDocumentIterator.nextDocument() );
		assertEquals( 2, andDocumentIterator.intervalIterator().extent() );
		assertEquals( 2, andDocumentIterator.nextDocument() );
		assertEquals( 2, andDocumentIterator.intervalIterator().extent() );
		assertEquals( END_OF_LIST, andDocumentIterator.nextDocument() );
		assertEquals( END_OF_LIST, andDocumentIterator.nextDocument() );
	}

	@Test
	public void testIteration_ND_NI() throws IOException {
		final long[] documents = new long[] { 0, 1, 2, 4 };
		final int[][][] firstIntervals = new int[][][] {
				{ { 0, 1 }, { 1, 2 } },
				{ { 0, 1 }, { 1, 2 } },
				{ { 1 }, { 2 } },
				{ { 0, 1 }, { 1, 2 } },
		};
		final int[][][] secondInterval = new int[][][] {
				{ { 5, 7 } },
				{ {} },
				{ { 2 }, { 3 }, { 4 } },
				{ },
		};
		final int[][][] expectedIntervalsArray = new int[][][] {
				{ { 1, 7 } },
				{ { 0, 1 }, { 1, 2} },
				{ { 2 } },
				{ { 0, 1 }, { 1, 2 } },
		};
		final IntArrayDocumentIterator i0 = new IntArrayDocumentIterator( documents, firstIntervals );
		final IntArrayDocumentIterator i1 = new IntArrayDocumentIterator( documents, secondInterval );
		final DocumentIterator andDocumentIterator = AndDocumentIterator.getInstance( i0, i1 );
		long document;
		int docCnt;
		for (docCnt = 0; (document = andDocumentIterator.nextDocument()) != END_OF_LIST; ++docCnt) {
			assertTrue("Too many documents", docCnt < documents.length);
			assertEquals("Doc id is invalid", documents[docCnt], document);
			final IntervalIterator intervalIterator = andDocumentIterator.intervalIterator();
			Interval interval;
			final int [][] expectedIntervals = expectedIntervalsArray[docCnt];
			int intCnt;
			for (intCnt = 0; (interval = intervalIterator.nextInterval()) != null; ++intCnt) {
				assertTrue("Too many intervals on doc #" + Integer.toString(intCnt), intCnt < expectedIntervals.length);
				final int [] boundaries = expectedIntervals[intCnt];
				int expectedLeft;
				int expectedRight;
				assertTrue("Unexpected boundaries found", boundaries.length > 0);
				if (boundaries.length == 1) {
					expectedLeft = expectedRight = boundaries[0];
				} else {
					expectedLeft  = boundaries[0];
					expectedRight = boundaries[1];
				}
				assertEquals(
						"Left boundary is wrong on interval #"  + Integer.toString(docCnt) + "/" + Integer.toString(intCnt),
						expectedLeft,
						interval.left
						);
				assertEquals(
						"Right boundary is wrong on interval #"  + Integer.toString(docCnt) + "/" + Integer.toString(intCnt),
						expectedRight,
						interval.right
						);
			}
			assertEquals("More intervals expected", expectedIntervals.length, intCnt);
		}
		assertEquals("More documents expected", documents.length, docCnt);
	}

	@Test
	public void testIteration_N_N() throws IOException {
		final long[] documents = new long[] { 0, 1, 2, 4 };
		final int[][][] firstIntervals = new int[][][] {
				{ { 0, 1 }, { 1, 2 } },
				{ { 0, 1 }, { 1, 2 } },
				{ { 1 }, { 2 } },
				{ { 0, 1 }, { 1, 2 } },
		};
		final int[][][] secondInterval = new int[][][] {
				{ { 5, 7 } },
				{ {} },
				{ { 2 }, { 3 }, { 4 } },
				{ },
		};
		final int[][][] expectedIntervalsArray = new int[][][] {
				{ { 1, 7 } },
						{ { 0, 1 }, { 1, 2} },
						{ { 2 } },
						{ { 0, 1 }, { 1, 2 } },
		};
		final IntArrayDocumentIterator i0 = new IntArrayDocumentIterator( documents, firstIntervals );
		final IntArrayDocumentIterator i1 = new IntArrayDocumentIterator( documents, secondInterval );
		final DocumentIterator andDocumentIterator = AndDocumentIterator.getInstance( i0, i1 );
		int docCnt = 0;
		for (; andDocumentIterator.mayHaveNext(); ++docCnt) {
			final long document = andDocumentIterator.nextDocument();
			if (document == END_OF_LIST) {
				break;
			}
			assertTrue("Too many documents", docCnt < documents.length);
			assertEquals("Doc id is invalid", documents[docCnt], document);
			final IntervalIterator intervalIterator = andDocumentIterator.intervalIterator();
			final int [][] expectedIntervals = expectedIntervalsArray[docCnt];
			int intCnt = 0;
			for ( Interval interval; ( interval = intervalIterator.nextInterval() ) != null; ++intCnt) {
				assertTrue("Too many intervals on doc #" + Integer.toString(intCnt), intCnt < expectedIntervals.length);
				final int [] boundaries = expectedIntervals[intCnt];
				int expectedLeft;
				int expectedRight;
				assertTrue("Unexpected boundaries found", boundaries.length > 0);
				if (boundaries.length == 1) {
					expectedLeft = expectedRight = boundaries[0];
				} else {
					expectedLeft  = boundaries[0];
					expectedRight = boundaries[1];
				}
				assertEquals(
						"Left boundary is wrong on interval #"  + Integer.toString(docCnt) + "/" + Integer.toString(intCnt),
						expectedLeft,
						interval.left
						);
				assertEquals(
						"Right boundary is wrong on interval #"  + Integer.toString(docCnt) + "/" + Integer.toString(intCnt),
						expectedRight,
						interval.right
						);
			}
			assertEquals("More intervals expected", expectedIntervals.length, intCnt);
		}
		assertEquals("More documents expected", documents.length, docCnt);
	}
}
