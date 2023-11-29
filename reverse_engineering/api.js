'use strict'

const { setDependencies, dependencies } = require('../shared/appDependencies');
const { adaptJsonSchema } = require('./adaptJsonSchema');
const { initPluginConfiguration } = require('../shared/customProperties');
const mapJsonSchema = require('../shared/mapJsonSchema');
const convertToJsonSchemas = require('./helpers/convertToJsonSchemas');
const { openAvroFile } = require('./helpers/fileHelper');
const { getNamespace, handleErrorObject } = require('./helpers/generalHelper');

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



const getPackages = (avroSchema, jsonSchemas) => {
	const schemasData = getSchemasData(avroSchema);
	const isAvroSchemaSplittedIntoMultipleJsonSchemas = schemasData.length === 1 && jsonSchemas.length > 1
	const singleNamespaceForOneAvroSchemaSplittedIntoMultipleJsonSchemas = schemasData[0]?.namespace

	return jsonSchemas.map((jsonSchema, index) => {
		const { namespace, schemaType, schemaGroupName, confluentSubjectName, schemaTopic, confluentVersion } = schemasData[index] || {};
		const schemaNamespace = isAvroSchemaSplittedIntoMultipleJsonSchemas ? singleNamespaceForOneAvroSchemaSplittedIntoMultipleJsonSchemas : namespace

		const schemaNameStrategy = inferSchemaNameStrategy({
			name: jsonSchema.title,
			namespace,
			confluentSubjectName,
			schemaTopic,
		});
		let references = [];
		mapJsonSchema(field => {
			if (!field.$ref) {
				return field;
			}

			const COLLECTION_REFERENCE_PREFIX = '#collection/definitions/';
			const isCollectionRef = field.$ref.startsWith(COLLECTION_REFERENCE_PREFIX);
			if (isCollectionRef) {
				references = [...references, field.$ref.slice(COLLECTION_REFERENCE_PREFIX.length)];
			}
			return field;
		})(jsonSchema);

		return {
			objectNames: {
				collectionName: jsonSchema.title,
			},
			doc: {
				dbName: schemaNamespace || '',
				collectionName: jsonSchema.title,
				bucketInfo: {
					name: schemaNamespace || '',
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
			references,
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

module.exports = { reFromFile, adaptJsonSchema };
