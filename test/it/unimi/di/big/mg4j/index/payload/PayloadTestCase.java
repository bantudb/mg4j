package it.unimi.di.big.mg4j.index.payload;

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

public abstract class PayloadTestCase {
	/** Checks that a given payload serialises correctly.
	 * 
	 * @param payload a payload containing a current value.
	 */
	public static void testWriteAndRead( Payload payload ) throws IOException {
		final FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		final OutputBitStream obs = new OutputBitStream( fbos );
		Object o = payload.get();
		payload.write( obs );
		obs.flush();
		final InputBitStream ibs = new InputBitStream( fbos.array );
		payload.read( ibs );
		assertEquals( o, payload.get() );
	}
}
