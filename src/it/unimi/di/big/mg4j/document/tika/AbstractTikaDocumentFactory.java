package  it.unimi.di.big.mg4j.document.tika;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2011-2016 Paolo Boldi and Sebastiano Vigna  
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.util.Properties;

import java.util.List;
import java.util.ListIterator;

import org.apache.commons.configuration.ConfigurationException;

/**
 * An abstract document factory that provides the mapping from field names to field indices.
 * 
 * <p>Concrete subclasses must implement the method {@link #fields()}, providing the list of Tika fields.
 * 
 * @author Salvatore Insalaco
 */
abstract public class AbstractTikaDocumentFactory extends PropertyBasedDocumentFactory {
	private static final long serialVersionUID = 1L;

	public AbstractTikaDocumentFactory( Properties properties ) throws ConfigurationException {
		super( properties );
	}

	public AbstractTikaDocumentFactory( Reference2ObjectMap<Enum<?>, Object> defaultMetadata ) {
		super( defaultMetadata );
	}

	public AbstractTikaDocumentFactory( String[] property ) throws ConfigurationException {
		super( property );
	}

	public AbstractTikaDocumentFactory() {
		super();
	}

	@Override
	public int numberOfFields() {
		return fields().size();
	}

	@Override
	public String fieldName( int field ) {
		return fields().get( field ).mg4jName();
	}

	@Override
	public int fieldIndex( String fieldName ) {
		// TODO: use a map
		ListIterator<TikaField> li = fields().listIterator();
		while ( li.hasNext() ) {
			if ( li.next().mg4jName().equals( fieldName ) ) return li.previousIndex();
		}
		return -1;
	}

	@Override
	public FieldType fieldType( int field ) {
		return fields().get( field ).getType();
	}

	/** Returns the list of Tika fields (they will be mapped to MG4J fields whose index is their index in the list).
	 * 
	 * @return the list of Tika fields.
	 */
	protected abstract List<TikaField> fields();
}
