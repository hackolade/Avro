'use strict'

const { setDependencies, dependencies } = require('../shared/appDependencies');
const jsonSchemaAdapter = require('./helpers/adaptJsonSchema');
const convertToJsonSchemas = require('./helpers/convertToJsonSchemas');
const { openAvroFile, getExtension } = require('./helpers/fileHelper');
const { getNamespace } = require('./helpers/generalHelper');

let _;

const reFromFile = async (data, logger, callback, app) => {
	setDependencies(app);
	_ = dependencies.lodash;
	try {
		const { filePath } = data;
		const avroSchema = await openAvroFile(filePath);
		const jsonSchemas = convertToJsonSchemas(avroSchema);

		if (jsonSchemas.length === 1) {
			const { schemaGroupName, namespace } = _.first(getSchemasData(avroSchema));

			return callback(null, {
				jsonSchema: JSON.stringify(_.first(jsonSchemas)),
				extension: getExtension(filePath),
				containerName: namespace,
				containerAdditionalData: { schemaGroupName }
			});
		}

		return callback(null, getPackages(avroSchema, jsonSchemas), {}, [],  'multipleSchema');
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

	return jsonSchemas.map((jsonSchema, index) => ({
		objectNames: {
			collectionName: jsonSchema.title,
		},
		doc: {
			dbName: schemasData[index]?.namespace || '',
			collectionName: jsonSchema.title,
			bucketInfo: {
				name: schemasData[index]?.namespace || '',
				schemaGroupName: schemasData[index]?.schemaGroupName || ''
			},
		},
		jsonSchema: JSON.stringify(jsonSchema),
	}));
};

const getSchemasData = avroSchema => {
	avroSchema = _.isArray(avroSchema) ? avroSchema : [ avroSchema ];

	return avroSchema.map(schema => ({
		namespace: getNamespace(schema) || '',
		schemaGroupName: schema.schemaGroupName,
		confluentSubjectName: schema.subject,
	}));
}

const handleErrorObject = error => _.pick(error, ['title', 'message', 'stack']);

module.exports = { reFromFile, adaptJsonSchema };
