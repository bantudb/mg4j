package it.unimi.di.big.mg4j.tool;

import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.dsi.util.Properties;

public final class Modulo3DocumentalClusteringStrategy implements DocumentalClusteringStrategy, DocumentalPartitioningStrategy {
	private static final long serialVersionUID = 1L;

	final int documents;

	public Modulo3DocumentalClusteringStrategy( final int documents ) {
		this.documents = documents;
	}

	public long globalPointer( int localIndex, long localPointer ) { return localPointer; }
	public long localPointer( long globalPointer ) { return globalPointer; }
	public long numberOfDocuments( int localIndex ) { return documents; }
	public int numberOfLocalIndices() { return 3; }
	public int localIndex( long globalPointer ) { return (int)( globalPointer % 3 ); }
	public Properties[] properties() { return new Properties[ 3 ]; }
}
