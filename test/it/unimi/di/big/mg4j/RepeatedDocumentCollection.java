package it.unimi.di.big.mg4j;

import it.unimi.di.big.mg4j.document.AbstractDocument;
import it.unimi.di.big.mg4j.document.AbstractDocumentCollection;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.IdentityDocumentFactory;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMaps;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

/** A document collection of given size, whose documents contain a given string repeated
 * for a given number of times.
 */

public class RepeatedDocumentCollection extends AbstractDocumentCollection {

	private int size;
	private int times;
	private IdentityDocumentFactory factory = new IdentityDocumentFactory();
	private WordReader wordReader = new FastBufferedReader();
	private final String repeat;

	public RepeatedDocumentCollection( int size, String repeat, int times ) {
		this.repeat = repeat;
		this.size = size;
		this.times = times;
	}

	public long size() {
		return size;
	}

	public Document document( final long index ) throws IOException {
		return new AbstractDocument() {

			public CharSequence title() {
				return "Document " + index;
			}

			public CharSequence uri() {
				return title();
			}

			public Object content( int field ) throws IOException {
				StringBuilder s = new StringBuilder();
				for( int i = times; i-- != 0; ) s.append( repeat );
					
				return new StringReader( s.toString() );
			}

			public WordReader wordReader( int field ) {
				return wordReader;
			}
			
		};
	}

	public InputStream stream( long index ) throws IOException {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	public Reference2ObjectMap<Enum<?>,Object> metadata( long index ) throws IOException {
		return Reference2ObjectMaps.EMPTY_MAP;
	}

	public RepeatedDocumentCollection copy() {
		return new RepeatedDocumentCollection( size, repeat, times );
	}

	public DocumentFactory factory() {
		return factory;
	}
}
