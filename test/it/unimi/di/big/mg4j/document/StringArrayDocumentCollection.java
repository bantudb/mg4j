package it.unimi.di.big.mg4j.document;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.commons.configuration.ConfigurationException;

/** A document collection explicitly defined by a sequence of strings (mainly useful for testing).
 * 
 * <p>Every string in the provided sequence is considered a document, 
 * and the stream returned by the collection is the UTF-8 encoding of the string. By default,
 * the collection uses an {@link IdentityDocumentFactory}, but you can also
 * {@linkplain #StringArrayDocumentCollection(DocumentFactory, String[]) specify your own factory}.
 */

public class StringArrayDocumentCollection extends AbstractDocumentCollection implements Serializable {
	private static final long serialVersionUID = 1L;
	final public String[] document;
	final DocumentFactory factory;
	private final boolean uris;

	public StringArrayDocumentCollection( final String... document ) throws ConfigurationException {
		this( false, document );
	}

	public StringArrayDocumentCollection( final DocumentFactory factory, final String... document ) {
		this( false, factory, document );
	}

	public StringArrayDocumentCollection( boolean uris, final String... document ) throws ConfigurationException {
		this( uris, new IdentityDocumentFactory( new String[] { "encoding=UTF-8" } ), document );
	}

	public StringArrayDocumentCollection( boolean uris, final DocumentFactory factory, final String... document ) {
		this.uris = uris;
		this.factory = factory;
		this.document = document;
	}

	public long size() {
		return document.length;
	}

	public Document document( final long index ) throws IOException {
		return factory.getDocument( stream( index ), metadata( index ) );
	}

	public InputStream stream( final long index ) throws IOException {
		return new FastByteArrayInputStream( document[ (int)index ].getBytes( "UTF-8" ) );
	}

	public Reference2ObjectMap<Enum<?>,Object> metadata( long index ) throws IOException {
		final Reference2ObjectArrayMap<Enum<?>, Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>(1);
		metadata.put( PropertyBasedDocumentFactory.MetadataKeys.TITLE, "Document " + index );
		if ( uris ) metadata.put( PropertyBasedDocumentFactory.MetadataKeys.URI, "doc:" + index );
		return metadata;
	}

	public DocumentCollection copy() {
		return this;
	}

	public DocumentFactory factory() {
		return factory;
	};
}
