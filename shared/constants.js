const META_VALUES_KEY_MAP = {
	'avro.java.string': 'metaValueString',
	'java-element': 'metaValueElement',
	'java-element-class': 'metaValueElementClass',
	'java-class': 'metaValueClass',
	'java-key-class': 'metaValueKeyClass'
};

const SCHEMA_REGISTRIES_KEYS = {
	'Confluent Schema Registry': 'CONFLUENT_SCHEMA_REGISTRY',
	'Azure Schema Registry': 'AZURE_SCHEMA_REGISTRY',
	'Pulsar Schema Registry': 'PULSAR_SCHEMA_REGISTRY',
};

const SCRIPT_TYPES = {
	CONFLUENT_SCHEMA_REGISTRY: 'confluentSchemaRegistry',
	AZURE_SCHEMA_REGISTRY: 'azureSchemaRegistry',
	PULSAR_SCHEMA_REGISTRY: 'pulsarSchemaRegistry',
	SCHEMA_REGISTRY: 'schemaRegistry',
};

const GENERAL_ATTRIBUTES = ['type', 'doc', 'order', 'default'];

const AVRO_TYPES = [
	'string',
	'boolean',
	'bytes',
	'null',
	'array',
	'record',
	'enum',
	'fixed',
	'int',
	'long',
	'float',
	'double',
	'map',
];

module.exports = {
	META_VALUES_KEY_MAP,
	SCRIPT_TYPES,
	SCHEMA_REGISTRIES_KEYS,
	GENERAL_ATTRIBUTES,
	AVRO_TYPES,
};
