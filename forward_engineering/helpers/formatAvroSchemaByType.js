const { dependencies } = require('../../shared/appDependencies');
const { SCRIPT_TYPES } = require('../../shared/constants');
const { reorderAttributes } = require('./generalHelper');

let _;

const formatAvroSchemaByType = ({ scriptType, settings, needMinify, isJsonFormat, avroSchema }) => {
	_ = dependencies.lodash;

	const formatter = getFormatter(scriptType);

	return formatter({
		settings,
		needMinify,
		isJsonFormat,
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
		case SCRIPT_TYPES.AZURE_SCHEMA_REGISTRY:
			return formatAzureSchemaRegistry;
		case SCRIPT_TYPES.PULSAR_SCHEMA_REGISTRY:
			return formatPulsarSchemaRegistry;
		default:
			return formatCommon;
	}
};

const formatConfluentSchema = ({ settings, needMinify, isJsonFormat, avroSchema }) => {
	const {
		name,
		namespace,
		confluentSubjectName,
		schemaType,
		schemaTopic,
		schemaNameStrategy,
		schemaRegistryUrl,
		confluentCompatibility,
		references,
	} = settings;

	return getConfluentPostQuery({
		schema: needMinify ? JSON.stringify(avroSchema) : avroSchema,
		isJsonFormat,
		name,
		namespace,
		confluentSubjectName,
		schemaType,
		schemaTopic,
		schemaNameStrategy,
		schemaRegistryUrl,
		confluentCompatibility,
		references,
	});
};

const getConfluentSubjectName = ({ name, namespace, schemaType, schemaTopic, schemaNameStrategy, confluentSubjectName }) => {
	const RECORD_NAME_STRATEGY = 'RecordNameStrategy';
	const TOPIC_NAME_STRATEGY = 'TopicNameStrategy';
	const TOPIC_RECORD_NAME_STRATEGY = 'TopicRecordNameStrategy';

	if (!schemaNameStrategy && confluentSubjectName) {
		return confluentSubjectName;
	}

	const fullName = [namespace, name].filter(Boolean).join('.');
	const typePostfix = schemaType || '';
	const topicPrefix = schemaTopic || '';

	switch(schemaNameStrategy){
		case RECORD_NAME_STRATEGY:
			return [fullName, typePostfix].filter(Boolean).join('-');
		case TOPIC_NAME_STRATEGY:
			return [topicPrefix || name, typePostfix].filter(Boolean).join('-');
		case TOPIC_RECORD_NAME_STRATEGY:
			return [topicPrefix, fullName, typePostfix].filter(Boolean).join('-');
		default:
			return [name, typePostfix].filter(Boolean).join('-');
	};
};

const getConfluentPostQuery = ({
	name,
	namespace, 
	schemaType,
	schemaTopic,
	schemaNameStrategy,
	confluentSubjectName,
	confluentCompatibility,
	isJsonFormat,
	references,
	schema,
}) => {
	const subjectName = getConfluentSubjectName({ name, namespace, schemaType, schemaTopic, schemaNameStrategy, confluentSubjectName });
	const compatibilityRequest = confluentCompatibility ? `PUT /config/${subjectName} HTTP/1.1\n{ "compatibility": "${confluentCompatibility}" }\n\n` : '';
	const requestBody = JSON.stringify(
		{ schema, schemaType: 'AVRO', ...(!_.isEmpty(references) && { references: _.uniqBy(references, 'name') }) },
		null,
		4
	);

	if (isJsonFormat) {
		return requestBody;
	}

	return `${compatibilityRequest}POST /subjects/${subjectName}/versions\n${requestBody}`;
};

const formatAzureSchemaRegistry = ({ settings, needMinify, isJsonFormat, avroSchema }) => {
	const { schemaGroupName, name } = settings;

	const requestBody = stringifyCommon(needMinify, avroSchema);

	if (isJsonFormat) {
		return requestBody;
	}

	return `PUT /${schemaGroupName}/schemas/${name}?api-version=2020-09-01-preview\n${requestBody}`;
};

const formatPulsarSchemaRegistry = ({ settings, needMinify, isJsonFormat, avroSchema }) => {
	const { persistence, namespace, topic } = settings;
	const bodyObject = {
		type: 'AVRO',
		data: avroSchema,
		properties: {}
	};
	const requestBody = stringifyCommon(needMinify, bodyObject);

	if (isJsonFormat) {
		return requestBody;
	}

	return `POST /${persistence}/${namespace}/${topic}/schema\n${requestBody}`;
};

const formatCommon = ({ needMinify, avroSchema }) => {
	return JSON.stringify(avroSchema, null, needMinify ? 0 : 4);
};

const stringifyCommon = (needMinify, schema) => {
	return needMinify ? JSON.stringify(schema) : JSON.stringify(schema, null, 4);
};

module.exports = { formatAvroSchemaByType, getConfluentSubjectName };
