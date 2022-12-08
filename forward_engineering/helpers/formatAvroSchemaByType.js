const { dependencies } = require('../../shared/appDependencies');
const { SCRIPT_TYPES } = require('../../shared/constants');
const { reorderAttributes } = require('./generalHelper');

let _;

const formatAvroSchemaByType = ({ scriptType, settings, needMinify, avroSchema }) => {
	_ = dependencies.lodash;

	const formatter = getFormatter(scriptType);

	return formatter({
		settings,
		needMinify,
		avroSchema: reorderAttributes({
			...avroSchema,
			name: settings.name,
			namespace: settings.namespace || avroSchema.namespace
		}),
	});
};

const getFormatter = scriptType => {
	switch (scriptType) {
		case SCRIPT_TYPES.CONFLUENT_SCHEMA_REGISTRY:
			return formatConfluentSchema;
		case SCRIPT_TYPES.SCHEMA_REGISTRY:
			return formatSchemaRegistry;
		case SCRIPT_TYPES.AZURE_SCHEMA_REGISTRY:
			return formatAzureSchemaRegistry;
		case SCRIPT_TYPES.PULSAR_SCHEMA_REGISTRY:
			return formatPulsarSchemaRegistry;
		default:
			return formatCommon;
	}
};

const formatConfluentSchema = ({ settings, needMinify, avroSchema }) => {
	const {
		name,
		namespace,
		confluentSubjectName,
		schemaType,
		schemaTopic,
		schemaNameStrategy,
		schemaRegistryUrl,
	} = settings;

	return getConfluentPostQuery({
		schema: needMinify ? JSON.stringify(avroSchema) : avroSchema,
		name,
		namespace,
		confluentSubjectName,
		schemaType,
		schemaTopic,
		schemaNameStrategy,
		schemaRegistryUrl,
	});
};

const getConfluentPostQuery = ({
	name,
	namespace, 
	schemaType,
	schemaTopic,
	schemaNameStrategy,
	schemaRegistryUrl,
	schema,
}) => {
	const RECORD_NAME_STRATEGY = 'RecordNameStrategy';
	const TOPIC_NAME_STRATEGY = 'TopicNameStrategy';
	const TOPIC_RECORD_NAME_STRATEGY = 'TopicRecordNameStrategy';

	const getName = () => {
		const typePostfix = schemaType ? `-${schemaType}` : '';
		const containerPrefix = namespace ? `${namespace}.`:'';
		const topicPrefix = schemaTopic ? `${schemaTopic}-`:'';

		switch(schemaNameStrategy){
			case RECORD_NAME_STRATEGY:
				return `${containerPrefix}${name}${typePostfix}`
			case TOPIC_NAME_STRATEGY:
				return `${topicPrefix}${typePostfix}`
			case TOPIC_RECORD_NAME_STRATEGY:
				return `${topicPrefix}${containerPrefix}${name}${typePostfix}`
			default:
				return `${name}${typePostfix}`;
		}
	}

	return `POST ${schemaRegistryUrl || ''}/subjects/${getName()}/versions\n${JSON.stringify(
		{ schema, schemaType: 'AVRO' },
		null,
		4
	)}`;
};

const formatSchemaRegistry = ({ needMinify, avroSchema }) => {
	return JSON.stringify({ schema: JSON.stringify(avroSchema) }, null, needMinify ? 0 : 4);
};

const formatAzureSchemaRegistry = ({ settings, needMinify, avroSchema }) => {
	const { schemaGroupName, schemaRegistryUrl, name } = settings;

	return `PUT ${schemaRegistryUrl || ''} /${schemaGroupName}/schemas/${name}?api-version=2020-09-01-preview\n${stringifyCommon(needMinify, avroSchema)}`;
};

const formatPulsarSchemaRegistry = ({ settings, needMinify, avroSchema }) => {
	const { persistence, namespace, topic, schemaRegistryUrl } = settings;
	const bodyObject = {
		type: 'AVRO',
		data: avroSchema,
		properties: {}
	};

	return `POST ${schemaRegistryUrl || ''}/${persistence}/${namespace}/${topic}/schema\n${stringifyCommon(needMinify, bodyObject)}`;
};

const formatCommon = ({ needMinify, avroSchema }) => {
	return JSON.stringify(avroSchema, null, needMinify ? 0 : 4);
};

const stringifyCommon = (needMinify, schema) => {
	return needMinify ? JSON.stringify(schema) : JSON.stringify(schema, null, 4);
};

module.exports = formatAvroSchemaByType;
