package it.unimi.di.big.mg4j.document;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMaps;
import it.unimi.dsi.io.WordReader;

import java.io.IOException;
import java.io.InputStream;

/** A document collection explicitly defined by a sequence of integers (mainly useful for testing).
 * 
 * <p>Every integer in the provided sequence is considered a document, and the only
 * field is of type integer. The factory is built-in.
 */

public class IntArrayDocumentCollection extends AbstractDocumentCollection {
	public final static class IntArrayDocumentFactory extends AbstractDocumentFactory {
		private static final long serialVersionUID = 1L;

		public DocumentFactory copy() { return this; }

		public int fieldIndex( String fieldName ) {
			if ( "int".equals( fieldName ) ) return 0;
			return -1;
		}

		public String fieldName( int field ) {
			ensureFieldIndex( field );
			return "int";
		}

		public FieldType fieldType( int field ) {
			ensureFieldIndex( field );
			return FieldType.INT;
		}

		public Document getDocument( InputStream rawContent, Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
			throw new UnsupportedOperationException();
		}

		public int numberOfFields() { return 1; }
	}

	final public int[] n;
	final DocumentFactory factory;

	public IntArrayDocumentCollection( final int... n ) {
		factory = new IntArrayDocumentFactory();
		
		this.n = n;
	}

	public long size() {
		return n.length;
	}

	public Document document( final long index ) {
		return new Document() {
			public void close() {}
			public Object content( int field ) throws IOException {
				ensureDocumentIndex( index );
				return Integer.valueOf( n[ (int)index ] ); 
			}
			public CharSequence title() { return null; }
			public CharSequence uri() { return null; }
			public WordReader wordReader( int field ) { throw new UnsupportedOperationException(); }
		};
	}

	public InputStream stream( final long index ) throws IOException {
		return new FastByteArrayInputStream( Integer.toString( n [ (int)index ] ).getBytes( "ASCII" ) );
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
