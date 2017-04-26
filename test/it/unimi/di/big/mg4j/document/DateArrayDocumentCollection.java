package it.unimi.di.big.mg4j.document;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMaps;
import it.unimi.dsi.io.WordReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/** A document collection explicitly defined by a sequence of {@linkplain Date dates} (mainly useful for testing).
 * 
 * <p>Every integer in the provided sequence is considered a document, and the only
 * field is of type integer. The factory is built-in.
 */

public class DateArrayDocumentCollection extends AbstractDocumentCollection {
	public final static class DateArrayDocumentFactory extends AbstractDocumentFactory {
		private static final long serialVersionUID = 1L;

		public DocumentFactory copy() { return this; }

		public int fieldIndex( String fieldName ) {
			if ( "date".equals( fieldName ) ) return 0;
			return -1;
		}

		public String fieldName( int field ) {
			ensureFieldIndex( field );
			return "date";
		}

		public FieldType fieldType( int field ) {
			ensureFieldIndex( field );
			return FieldType.DATE;
		}

		public Document getDocument( InputStream rawContent, Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
			throw new UnsupportedOperationException();
		}

		public int numberOfFields() { return 1; }
	}

	final public Date[] date;
	final DocumentFactory factory;

	public DateArrayDocumentCollection( final Date... date ) {
		factory = new DateArrayDocumentFactory();
		
		this.date = date;
	}

	public long size() {
		return date.length;
	}

	public Document document( final long index ) {
		return new Document() {
			public void close() {}
			public Object content( int field ) throws IOException {
				ensureDocumentIndex( index );
				return date[ (int)index ]; 
			}
			public CharSequence title() { return null; }
			public CharSequence uri() { return null; }
			public WordReader wordReader( int field ) { throw new UnsupportedOperationException(); }
		};
	}

	public InputStream stream( final long index ) throws IOException {
		return new FastByteArrayInputStream( date[ (int)index ].toString().getBytes( "ASCII" ) );
	}

	@SuppressWarnings("unchecked")
	public Reference2ObjectMap<Enum<?>, Object> metadata( long index ) throws IOException {
		return Reference2ObjectMaps.EMPTY_MAP;
	}

	public DocumentCollection copy() {
		return this;
	}

	public DocumentFactory factory() {
		return factory;
	};
}
