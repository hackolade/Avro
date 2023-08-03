'use strict'

const { setDependencies, dependencies } = require('../shared/appDependencies');
const { SCRIPT_TYPES, SCHEMA_REGISTRIES_KEYS } = require('../shared/constants');
const { parseJson, prepareName } = require('./helpers/generalHelper');
const validateAvroScript = require('./helpers/validateAvroScript');
const { formatAvroSchemaByType } = require('./helpers/formatAvroSchemaByType');
const { resolveUdt, addDefinitions, resetDefinitionsUsage, convertCollectionReferences, resolveNamespaceReferences } = require('./helpers/udtHelper');
const convertSchema = require('./helpers/convertJsonSchemaToAvro');
const { initPluginConfiguration, getCustomProperties, getEntityLevelConfig, getFieldLevelConfig } = require('../shared/customProperties');
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

		setUserDefinedTypes(externalDefinitions);
		setUserDefinedTypes(modelDefinitions);

		const entities = (containers || [])
			.flatMap(container => container.entities
			.map(entityId => getEntityData(container, entityId)))
			.map(entity => ({ ...entity, jsonSchema: parseJson(entity.jsonSchema) }));

		const script = handleCollectionReferences(entities, options).map(entity => {
			try {
				const {
					containerData,
					entityData,
					jsonSchema,
					internalDefinitions,
					references,
				} = entity;
				setUserDefinedTypes(internalDefinitions);
				resetDefinitionsUsage();

				const settings = getSettings({ containerData, entityData, modelData, references });

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
		setUserDefinedTypes(modelDefinitions)
		setUserDefinedTypes(internalDefinitions);
		resetDefinitionsUsage();

		const settings = getSettings({ containerData, entityData, modelData });
		const resolvedJsonSchema = _.first(handleCollectionReferences([{ jsonSchema: parseJson(jsonSchema) }], options)).jsonSchema;
		const script = getScript({
			scriptType: getEntityScriptType(options),
			needMinify: isMinifyNeeded(options),
			settings,
			avroSchema: convertJsonToAvro(resolvedJsonSchema, settings.name),
		});

		cb(null, script);
	} catch (err) {
		logger.log('error', { message: err.message, stack: err.stack }, 'Avro Forward-Engineering Error');
		cb({ message: err.message, stack: err.stack });
	}
};

const validate = (data, logger, cb, app) => {
	setDependencies(app);
	initPluginConfiguration(data.pluginConfiguration);
	_ = dependencies.lodash;

	const targetScript = data.script;
	const modelData = data.modelData[0] || {};
	let scriptType = getScriptType(data, modelData);
	if (!scriptType && targetScript.startsWith('POST /')) {
		scriptType = SCRIPT_TYPES.CONFLUENT_SCHEMA_REGISTRY;
	}
	const validationMessages = validateAvroScript(targetScript, scriptType, logger);

	return cb(null, validationMessages);
};

const getScriptType = (options, modelData) => options?.targetScriptOptions?.keyword || options?.targetScriptOptions?.format || SCRIPT_TYPES[SCHEMA_REGISTRIES_KEYS[modelData?.schemaRegistryType]];
const getEntityScriptType = options => options.origin === 'ui' && options?.targetScriptOptions?.keyword || SCRIPT_TYPES.COMMON;

const getEntityData = (container, entityId) => {
	const containerData = _.first(_.get(container, 'containerData', []));
	const jsonSchema = container.jsonSchema[entityId];
	const jsonData = container.jsonData[entityId];
	const entityData = _.first(container.entityData[entityId]);
	const internalDefinitions = container.internalDefinitions[entityId];

	return { containerData, jsonSchema, jsonData, entityData, internalDefinitions }
}

const convertJsonToAvro = (jsonSchema, schemaName) => {
	jsonSchema = { ...jsonSchema, name: schemaName, type: 'record' };
	const customProperties = getCustomProperties(getEntityLevelConfig(), jsonSchema);
	const schema = convertSchema(jsonSchema);
	const avroSchema = {
		...(_.isString(schema) ? { type: schema } : schema),
		name: schemaName,
		type: 'record',
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
			customProperties,
		}
	});

	return definitions.reduce((result, { name, schema, customProperties }) => ({
		...result,
		[name]: { schema, customProperties },
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

const getSettings = ({ containerData, entityData, modelData, references, }) => {
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

	return convertCollectionReferences(entities);
};

const isMinifyNeeded = options => {
	const additionalOptions = options?.additionalOptions || [];

	return additionalOptions.find(option => option.id === 'minify')?.value;
};

const isResolveNamespaceReferenceNeeded = options => {
	const additionalOptions = options?.additionalOptions || [];

	return additionalOptions.find(option => option.id === 'resolveNamespaceReferences')?.value;
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
