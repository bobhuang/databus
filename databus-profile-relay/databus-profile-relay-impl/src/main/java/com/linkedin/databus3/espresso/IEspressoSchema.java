package com.linkedin.databus3.espresso;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus2.schemas.SchemaId;

public interface IEspressoSchema {

	abstract Schema getSchema();
	
	abstract SchemaId getSchemaId();

	abstract short getSrcId();
	
	abstract GenericRecord createDataPerSchema();
}
