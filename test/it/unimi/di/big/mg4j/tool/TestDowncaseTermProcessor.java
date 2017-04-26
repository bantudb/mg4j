package it.unimi.di.big.mg4j.tool;

import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.index.DowncaseTermProcessor;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.IOException;

import org.junit.Test;

public class TestDowncaseTermProcessor {

	@Test
	public void testReadResolve() throws IOException, ClassNotFoundException {
		TermProcessor t = DowncaseTermProcessor.getInstance();
		FastByteArrayOutputStream os = new FastByteArrayOutputStream();
		BinIO.storeObject( t, os );
		assertTrue( t == (TermProcessor)BinIO.loadObject( new FastByteArrayInputStream( os.array ) ) );
	}
}
