'use strict'

const { setDependencies, dependencies } = require('../shared/appDependencies');
const { SCRIPT_TYPES } = require('../shared/constants');
const { parseJson, prepareName } = require('./helpers/generalHelper');
const validateAvroScript = require('./helpers/validateAvroScript');
const formatAvroSchemaByType = require('./helpers/formatAvroSchemaByType');
const { resolveUdt, addDefinitions, resetDefinitionsUsage } = require('./helpers/udtHelper');
const convertSchema = require('./helpers/convertJsonSchemaToAvro');
let _;

const generateModelScript = (data, logger, cb, app) => {
	logger.clear();
	try {
		setDependencies(app);
		_ = dependencies.lodash;

		const { containers, externalDefinitions, modelDefinitions, options, targetScriptOptions } = data;
		const scriptType = targetScriptOptions.format || SCRIPT_TYPES.CONFLUENT_SCHEMA_REGISTRY;
		const needMinify = isMinifyNeeded(options);
		const modelData = data.modelData[0] || {};

		setUserDefinedTypes(externalDefinitions);
		setUserDefinedTypes(modelDefinitions);

		const script = (containers || []).flatMap(container => {
			const containerEntities = container.entities.map(entityId => getEntityData(container, entityId));

			return containerEntities.map(entity => {
				try {
					const {
						containerData,
						entityData,
						jsonSchema,
						internalDefinitions,
					} = entity;

					setUserDefinedTypes(internalDefinitions);
					resetDefinitionsUsage();

					const settings = getSettings({ containerData, entityData, modelData });

					return getScript({ 
						scriptType,
						needMinify,
						settings,
						avroSchema: convertJsonToAvro(jsonSchema, settings.name),
					})
				} catch (err) {
					logger.log('error', { message: err.message, stack: err.stack }, 'Avro Forward-Engineering Error');
					return '';
				}
			})
		});
		cb(null, script.filter(Boolean).join('\n\n'));
	} catch (err) {
		logger.log('error', { message: err.message, stack: err.stack }, 'Avro model Forward-Engineering Error');
		cb({ message: err.message, stack: err.stack });
	}
};

const generateScript = (data, logger, cb, app) => {
	logger.clear();
	try {
		setDependencies(app);
		_ = dependencies.lodash;

		const {
			containerData,
			entityData,
			modelData,
			jsonSchema,
			options,

			internalDefinitions,
			externalDefinitions,
			modelDefinitions,
		} = data;

		setUserDefinedTypes(externalDefinitions);
		setUserDefinedTypes(modelDefinitions)
		setUserDefinedTypes(internalDefinitions);
		resetDefinitionsUsage();

		const settings = getSettings({ containerData, entityData, modelData });
		const script = getScript({
			scriptType: getScriptType(options),
			needMinify: isMinifyNeeded(options),
			settings,
			avroSchema: convertJsonToAvro(jsonSchema, settings.name),
		});

		cb(null, script);
	} catch (err) {
		logger.log('error', { message: err.message, stack: err.stack }, 'Avro Forward-Engineering Error');
		cb({ message: err.message, stack: err.stack });
	}
};

const validate = (data, logger, cb, app) => {
	setDependencies(app);
	_ = dependencies.lodash;

	const targetScript = data.script;
	const scriptType = data.targetScriptOptions.keyword || data.targetScriptOptions.format;
	const validationMessages = validateAvroScript(targetScript, scriptType, logger);

	return cb(null, validationMessages);
};

const getScriptType = options => options?.targetScriptOptions?.keyword;

const getEntityData = (container, entityId) => {
	const containerData = _.first(_.get(container, 'containerData', []));
	const jsonSchema = container.jsonSchema[entityId];
	const jsonData = container.jsonData[entityId];
	const entityData = _.first(container.entityData[entityId]);
	const internalDefinitions = container.internalDefinitions[entityId];

	return { containerData, jsonSchema, jsonData, entityData, internalDefinitions }
}

const convertJsonToAvro = (jsonString, schemaName) => {
	const jsonSchema = { ...parseJson(jsonString), type: 'record' };
	const avroSchema = {
		...convertSchema(jsonSchema),
		name: schemaName,
		type: 'record',
	};

	return resolveUdt(reorderAvroSchema(avroSchema));
};

const setUserDefinedTypes = definitions => {
	addDefinitions(convertSchemaToUserDefinedTypes(definitions));
};

const convertSchemaToUserDefinedTypes = definitionsSchema => {
	definitionsSchema = parseJson(definitionsSchema);
	const definitions = Object.keys(definitionsSchema.properties || {}).map(key => ({
		name: prepareName(key),
		schema: convertSchema(definitionsSchema.properties[key]),
	}));

	return definitions.reduce((result, { name, schema }) => ({
		...result,
		[name]: schema
	}), {});
};

const getScript = ({
	settings,
	scriptType,
	needMinify,
	avroSchema,
}) => {
	return formatAvroSchemaByType({
		avroSchema,
		scriptType,
		needMinify,
		settings,
	});
};

const getSettings = ({ containerData, entityData, modelData, }) => {
	return {
		name: getRootRecordName(entityData),
		namespace: containerData?.name || '',
		topic: entityData?.pulsarTopicName || '',
		persistence: entityData?.isNonPersistentTopic ? 'non-persistent' : 'persistent',
		schemaGroupName: containerData?.schemaGroupName || '',
		schemaType: entityData?.schemaType || '',
		schemaTopic: modelData?.schemaTopic || '',
		schemaNameStrategy: modelData?.schemaNameStrategy || '',
	};
};

const isMinifyNeeded = options => {
	const additionalOptions = options?.additionalOptions || [];

	return (additionalOptions.find(option => option.id === 'minify') || {}).value;
};

const getRootRecordName = entityData => prepareName(entityData.code || entityData.name || entityData.collectionName);

const reorderAvroSchema = avroSchema => setPropertyAsLast('fields')(avroSchema);

const setPropertyAsLast = key => avroSchema => {
	return { ..._.omit(avroSchema, key), [key]: avroSchema[key] };
};

module.exports = {
	generateModelScript,
	generateScript,
	validate,
};
