'use strict';

const { setDependencies, dependencies } = require('../shared/appDependencies');
const { SCRIPT_TYPES, SCHEMA_REGISTRIES_KEYS } = require('../shared/constants');
const { parseJson, prepareName } = require('./helpers/generalHelper');
const validateAvroScript = require('./helpers/validateAvroScript');
const { formatAvroSchemaByType, getConfluentSubjectName } = require('./helpers/formatAvroSchemaByType');
const {
	resolveUdt,
	addDefinitions,
	resetDefinitionsUsage,
	convertCollectionReferences,
	resolveNamespaceReferences,
	clearDefinitions,
} = require('./helpers/udtHelper');
const convertSchema = require('./helpers/convertJsonSchemaToAvro');
const {
	initPluginConfiguration,
	getCustomProperties,
	getEntityLevelConfig,
	getFieldLevelConfig,
} = require('../shared/customProperties');
let _;

const generateModelScript = (data, logger, cb, app) => {
	logger.clear();
	try {
		setDependencies(app);
		initPluginConfiguration(data.pluginConfiguration, logger);
		_ = dependencies.lodash;

		const { containers, externalDefinitions, modelDefinitions, options } = data;

		const modelData = data.modelData[0] || {};
		const scriptType = getScriptType(data, modelData) || SCRIPT_TYPES.CONFLUENT_SCHEMA_REGISTRY;
		const needMinify = isMinifyNeeded(options);

		const convertedExternalDefinitions = convertSchemaToUserDefinedTypes(externalDefinitions);
		const convertedModelDefinitions = convertSchemaToUserDefinedTypes(modelDefinitions);

		const entities = (containers || [])
			.flatMap(container => container.entities.map(entityId => getEntityData(container, entityId)))
			.map(entity => ({ ...entity, jsonSchema: parseJson(entity.jsonSchema) }));

		const script = handleCollectionReferences(entities, options).map(entity => {
			try {
				const { containerData, entityData, jsonSchema, internalDefinitions, references } = entity;

				clearDefinitions();
				addDefinitions(convertedExternalDefinitions);
				addDefinitions(convertedModelDefinitions);
				setUserDefinedTypes(internalDefinitions);
				resetDefinitionsUsage();

				const settings = getSettings({ containerData, entityData, modelData, references });

				return getScript({
					scriptType,
					needMinify,
					settings,
					avroSchema: convertJsonToAvro(jsonSchema, settings.name),
				});
			} catch (err) {
				logger.log('error', { message: err.message, stack: err.stack }, 'Avro Forward-Engineering Error');
				return '';
			}
		});

		const jsonData = combineJsonData(data.containers);
		const resultScript = script.filter(Boolean).join('\n\n');
		const isSampleGenerationRequired = includeSamplesToScript(data.options);
		if (!isSampleGenerationRequired) {
			return cb(null, resultScript);
		}

		return cb(null, getScriptAndSampleResponse(resultScript, jsonData));
	} catch (err) {
		logger.log('error', { message: err.message, stack: err.stack }, 'Avro model Forward-Engineering Error');
		cb({ message: err.message, stack: err.stack });
	}
};

const generateScript = (data, logger, cb, app) => {
	logger.clear();
	try {
		setDependencies(app);
		initPluginConfiguration(data.pluginConfiguration, logger);
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
		setUserDefinedTypes(modelDefinitions);
		setUserDefinedTypes(internalDefinitions);
		resetDefinitionsUsage();
		const isFromUi = options.origin === 'ui';

		const { references, jsonSchema: resolvedJsonSchema } =
			_.first(handleCollectionReferences([{ jsonSchema: parseJson(jsonSchema) }], options)) || {};
		const settings = getSettings({ containerData, entityData, modelData, references });
		const script = getScript({
			scriptType: getEntityScriptType(options, modelData),
			needMinify: isMinifyNeeded(options),
			isJsonFormat: !isFromUi,
			settings,
			avroSchema: convertJsonToAvro(resolvedJsonSchema, settings.name),
		});

		if (!includeSamplesToScript(options)) {
			const scriptType = options?.targetScriptOptions?.keyword;
			const isCliSchemaRegistryFormat =
				!isFromUi &&
				[
					SCRIPT_TYPES.CONFLUENT_SCHEMA_REGISTRY,
					SCRIPT_TYPES.AZURE_SCHEMA_REGISTRY,
					SCRIPT_TYPES.PULSAR_SCHEMA_REGISTRY,
				].includes(scriptType);

			const isSchemaRegistry = scriptType === SCRIPT_TYPES.SCHEMA_REGISTRY || isCliSchemaRegistryFormat;
			if (!isSchemaRegistry) {
				return cb(null, script);
			}

			return cb(null, [
				{
					title: 'Avro schemas',
					fileName: getConfluentSubjectName(settings),
					script,
				},
			]);
		}

		return cb(null, getScriptAndSampleResponse(script, data.jsonData));
	} catch (err) {
		logger.log('error', { message: err.message, stack: err.stack }, 'Avro Forward-Engineering Error');
		cb({ message: err.message, stack: err.stack });
	}
};

const validate = (data, logger, cb, app) => {
	setDependencies(app);
	initPluginConfiguration(data.pluginConfiguration);
	_ = dependencies.lodash;

	const targetScript = _.isArray(data.script) ? _.first(data.script)?.script : data.script;
	const modelData = data.modelData[0] || {};
	let scriptType = getScriptType(data, modelData);
	if (!scriptType && targetScript.startsWith('POST /')) {
		scriptType = SCRIPT_TYPES.CONFLUENT_SCHEMA_REGISTRY;
	}
	const validationMessages = validateAvroScript(targetScript, scriptType, logger);

	return cb(null, validationMessages);
};

const getScriptType = (options, modelData) => {
	if (options?.targetScriptOptions?.keyword === SCRIPT_TYPES.SCHEMA_REGISTRY) {
		return SCRIPT_TYPES[SCHEMA_REGISTRIES_KEYS[modelData?.schemaRegistryType]];
	}

	return (
		options?.targetScriptOptions?.keyword ||
		options?.targetScriptOptions?.format ||
		SCRIPT_TYPES[SCHEMA_REGISTRIES_KEYS[modelData?.schemaRegistryType]]
	);
};
const getEntityScriptType = (options, modelData) => {
	if (options?.targetScriptOptions?.keyword === SCRIPT_TYPES.SCHEMA_REGISTRY) {
		return SCRIPT_TYPES[SCHEMA_REGISTRIES_KEYS[modelData?.schemaRegistryType]];
	}

	return options?.targetScriptOptions?.keyword || SCRIPT_TYPES.COMMON;
};

const getEntityData = (container, entityId) => {
	const containerData = _.first(_.get(container, 'containerData', []));
	const jsonSchema = container.jsonSchema[entityId];
	const jsonData = container.jsonData[entityId];
	const entityData = _.first(container.entityData[entityId]);
	const internalDefinitions = container.internalDefinitions[entityId];

	return { containerData, jsonSchema, jsonData, entityData, internalDefinitions };
};

const convertJsonToAvro = (jsonSchema, schemaName) => {
	jsonSchema = { ...jsonSchema, name: schemaName, type: 'record' };
	const customProperties = getCustomProperties(getEntityLevelConfig(), jsonSchema);
	const schema = convertSchema(jsonSchema);
	if (Array.isArray(schema)) {
		return schema;
	}
	const avroSchema = {
		...(!_.isString(schema) && schema),
		name: schemaName,
		type: _.isString(schema) ? schema : 'record',
		...customProperties,
	};

	return resolveUdt(reorderAvroSchema(avroSchema));
};

const setUserDefinedTypes = definitions => {
	addDefinitions(convertSchemaToUserDefinedTypes(definitions));
};

const convertSchemaToUserDefinedTypes = definitionsSchema => {
	definitionsSchema = parseJson(definitionsSchema);
	const definitions = Object.keys(definitionsSchema.properties || {}).map(key => {
		const definition = definitionsSchema.properties[key];
		const customProperties = getCustomProperties(getFieldLevelConfig(definition.type), definition);

		return {
			name: prepareName(key),
			schema: convertSchema(definition),
			originalSchema: definition,
			customProperties,
		};
	});

	return definitions.reduce(
		(result, { name, schema, customProperties, originalSchema }) => ({
			...result,
			[name]: { schema, customProperties, originalSchema },
		}),
		{},
	);
};

const getScript = ({ settings, scriptType, isJsonFormat, needMinify, avroSchema }) => {
	return formatAvroSchemaByType({
		avroSchema,
		scriptType,
		isJsonFormat,
		needMinify,
		settings,
	});
};

const getSettings = ({ containerData, entityData, modelData, references }) => {
	return {
		name: getRootRecordName(entityData),
		namespace: containerData?.name || '',
		topic: entityData?.pulsarTopicName || '',
		persistence: entityData?.isNonPersistentTopic ? 'non-persistent' : 'persistent',
		schemaGroupName: containerData?.schemaGroupName || '',
		confluentSubjectName: entityData?.confluentSubjectName || '',
		confluentCompatibility: entityData?.confluentCompatibility || '',
		schemaType: entityData?.schemaType || '',
		schemaTopic: entityData?.schemaTopic || '',
		schemaNameStrategy: entityData?.schemaNameStrategy || '',
		schemaRegistryType: modelData?.schemaRegistryType || '',
		schemaRegistryUrl: modelData?.schemaRegistryUrl || '',
		references: references || [],
	};
};

const handleCollectionReferences = (entities, options) => {
	if (isResolveNamespaceReferenceNeeded(options)) {
		return resolveNamespaceReferences(entities);
	}

	return convertCollectionReferences(entities, options);
};

const isMinifyNeeded = options => {
	const additionalOptions = options?.additionalOptions || [];

	return additionalOptions.find(option => option.id === 'minify')?.value;
};

const isResolveNamespaceReferenceNeeded = options => {
	const additionalOptions = options?.additionalOptions || [];

	return additionalOptions.find(option => option.id === 'resolveEntityReferences')?.value;
};

const getRootRecordName = entityData => prepareName(entityData.code || entityData.name || entityData.collectionName);

const reorderAvroSchema = avroSchema => setPropertyAsLast('fields')(avroSchema);

const setPropertyAsLast = key => avroSchema => {
	return { ..._.omit(avroSchema, key), [key]: avroSchema[key] };
};

const includeSamplesToScript = (options = {}) =>
	!options?.targetScriptOptions?.cliOnly &&
	(options.additionalOptions || []).find(option => option.id === 'INCLUDE_SAMPLES')?.value;

const getScriptAndSampleResponse = (script, sample) => {
	return [
		{
			title: 'Avro schemas',
			script,
		},
		{
			title: 'Sample data',
			script: sample,
		},
	];
};

const combineJsonData = containersData => {
	const parsedData = containersData.flatMap(containerData => Object.values(containerData.jsonData)).map(JSON.parse);
	return JSON.stringify(parsedData, null, 4);
};

module.exports = {
	generateModelScript,
	generateScript,
	validate,
};
