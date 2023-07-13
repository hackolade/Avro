'use strict'

const { setDependencies, dependencies } = require('../shared/appDependencies');
const { initPluginConfiguration } = require('../shared/customProperties');
const jsonSchemaAdapter = require('./helpers/adaptJsonSchema');
const convertToJsonSchemas = require('./helpers/convertToJsonSchemas');
const { openAvroFile } = require('./helpers/fileHelper');
const { getNamespace } = require('./helpers/generalHelper');

let _;

const reFromFile = async (data, logger, callback, app) => {
	setDependencies(app);
	initPluginConfiguration(data.pluginConfiguration, logger);
	_ = dependencies.lodash;
	try {
		const { filePath } = data;
		const avroSchema = await openAvroFile(filePath);
		const jsonSchemas = convertToJsonSchemas(avroSchema);

		const { schemaRegistryType, schemaRegistryUrl } = _.first(getSchemasData(avroSchema));

		return callback(null, getPackages(avroSchema, jsonSchemas), {
			schemaRegistryType,
			schemaRegistryUrl,
		}, [],  'multipleSchema');
	} catch (err) {
		const errorData = handleErrorObject(err);
		logger.log('error', errorData, 'Parsing Avro Schema Error');

		return callback(errorData);
	}
};

const adaptJsonSchema = (data, logger, callback, app) => {
	setDependencies(app);
	_ = dependencies.lodash;

	logger.log('info', 'Adaptation of JSON Schema started...', 'Adapt JSON Schema');
	try {
		const jsonSchema = JSON.parse(data.jsonSchema);
		const adaptedJsonSchema = jsonSchemaAdapter.adaptJsonSchema(jsonSchema);

		logger.log('info', 'Adaptation of JSON Schema finished.', 'Adapt JSON Schema');

		callback(null, {
			jsonSchema: JSON.stringify(adaptedJsonSchema),
			jsonSchemaName: jsonSchemaAdapter.adaptJsonSchemaName(data.jsonSchemaName),
		});
	} catch(error) {
		callback({ ...handleErrorObject(error), title: 'Adapt JSON Schema' });
	}
};

const getPackages = (avroSchema, jsonSchemas) => {
	const schemasData = getSchemasData(avroSchema);

	return jsonSchemas.map((jsonSchema, index) => {
		const { namespace, schemaType, schemaGroupName, confluentSubjectName, schemaTopic, confluentVersion } = schemasData[index] || {};
		const schemaNameStrategy = inferSchemaNameStrategy({
			name: jsonSchema.title,
			namespace,
			confluentSubjectName,
			schemaTopic,
		});

		return {
			objectNames: {
				collectionName: jsonSchema.title,
			},
			doc: {
				dbName: namespace || '',
				collectionName: jsonSchema.title,
				bucketInfo: {
					name: namespace || '',
				},
			},
			jsonSchema: JSON.stringify({
				...jsonSchema,
				schemaType: schemaType,
				schemaTopic: schemaTopic,
				schemaGroupName: schemaGroupName,
				confluentSubjectName: confluentSubjectName,
				confluentVersion: confluentVersion,
				...(schemaNameStrategy && { schemaNameStrategy }), 
			}),
		};
	});
};

const inferSchemaNameStrategy = ({ name, namespace, confluentSubjectName, schemaTopic }) => {
	let splittedSubjectName = (confluentSubjectName || '').split('-').filter(Boolean);
	const endsWithSchemaType = ['key', 'value'].includes(_.last(splittedSubjectName));
	const startsWithTopic = _.first(splittedSubjectName) === schemaTopic && schemaTopic !== namespace + '.' + name;
	const startsWithNamespace = namespace && _.first(splittedSubjectName)?.startsWith(namespace + '.');

	if (startsWithNamespace) {
		splittedSubjectName[0] = splittedSubjectName[0].slice(namespace.length + 1);
	}

	if (endsWithSchemaType) {
		splittedSubjectName = splittedSubjectName.slice(0, -1);
	}

	if (startsWithTopic) {
		splittedSubjectName = splittedSubjectName.slice(1);
	}

	if (startsWithTopic && _.isEmpty(splittedSubjectName)) {
		if (name === schemaTopic) {
			return 'RecordNameStrategy';
		}

		return 'TopicNameStrategy';
	}

	const splittedRecordName = [...(name || '').split('-')].filter(Boolean);
	const recordNameStrategy = _.isEqual(splittedRecordName, splittedSubjectName);

	if (!recordNameStrategy) {
		return;
	}

	return startsWithTopic ? 'TopicRecordNameStrategy' : 'RecordNameStrategy';
};

const getSchemasData = avroSchema => {
	avroSchema = _.isArray(avroSchema) ? avroSchema : [ avroSchema ];

	return avroSchema.map(schema => ({
		namespace: getNamespace(schema) || '',
		schemaGroupName: schema.schemaGroupName,
		schemaRegistryType: schema.schemaRegistryType,
		schemaRegistryUrl: schema.schemaRegistryUrl,
		confluentSubjectName: schema.confluentSubjectName,
		schemaTopic: schema.schemaTopic,
		schemaType: schema.schemaType,
		confluentVersion: schema.version,
	}));
}

const handleErrorObject = error => _.pick(error, ['title', 'message', 'stack']);

module.exports = { reFromFile, adaptJsonSchema };
