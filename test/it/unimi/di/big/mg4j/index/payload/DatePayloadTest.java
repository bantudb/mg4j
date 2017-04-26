package it.unimi.di.big.mg4j.index.payload;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;

import org.junit.Test;

public class DatePayloadTest extends PayloadTestCase {
	
	@Test
	public void testReadWrite() throws IOException, ParseException {
		DatePayload datePayload = new DatePayload();
		Date date = DateFormat.getDateInstance( DateFormat.SHORT, Locale.US ).parse( "1/1/2001" );
		datePayload.set( date );
		testWriteAndRead( datePayload );
		assertEquals( date, datePayload.get() );

		date = DateFormat.getDateInstance( DateFormat.SHORT, Locale.US ).parse( "1/1/1901" );
		datePayload.set( date );
		testWriteAndRead( datePayload );
		assertEquals( date, datePayload.get() );
	}

}
