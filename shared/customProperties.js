const { AVRO_TYPES } = require('./constants');
const { dependencies } = require('./appDependencies');

let pluginConfiguration = {};
let logger = {};

const initPluginConfiguration = (config, appLogger) => {
	logger = appLogger;
	pluginConfiguration = config || {};

	const customPropertiesConfigMap = getCustomPropertiesConfigMap();
	if (!Object.keys(customPropertiesConfigMap).length) {
		return;
	}

	logger?.log(
		'info',
		`Plugin custom properties:\n${JSON.stringify(customPropertiesConfigMap, null, 4)}`,
		'Custom Properties detected',
	);
};

const getCustomPropertiesConfigMap = () => {
	const entityCustomProperties = getCustomPropertiesConfig(getEntityLevelConfig());

	return AVRO_TYPES.reduce(
		(typeToCustomProperties, type) => {
			const customProperties = getCustomPropertiesConfig(getFieldLevelConfig(type));
			if (!customProperties.length) {
				return typeToCustomProperties;
			}

			return {
				...typeToCustomProperties,
				[type]: customProperties,
			};
		},
		{
			...(entityCustomProperties.length && { entity: entityCustomProperties }),
		},
	);
};

const getCustomPropertiesConfig = config => config.filter(property => property.includeInScript);

const filterConfigByDependency = (config, attributes) => {
	return config.filter(property => {
		if (!property.dependency) {
			return true;
		}

		const keyValueDependency = property.dependency.key && property.dependency.value;
		if (!keyValueDependency) {
			return true;
		}

		return attributes[property.dependency.key] === property.dependency.value;
	});
};

const getCustomPropertiesKeywords = (config, attributes = {}) => {
	return filterConfigByDependency(getCustomPropertiesConfig(config), attributes).map(
		property => property.fieldKeyword,
	);
};

const getCustomProperties = (config, attributes = {}) => {
	return dependencies.lodash.pick(attributes, getCustomPropertiesKeywords(config, attributes));
};

const getFieldLevelConfig = type => {
	if (Array.isArray(type)) {
		return type.flatMap(getFieldLevelConfig);
	}

	const customTabConfig =
		pluginConfiguration.fieldLevelConfig?.tabs
			?.filter(tab => tab.customTab)
			.flatMap(tab => tab.structure?.[type] || []) || [];

	return [...(pluginConfiguration.fieldLevelConfig?.structure?.[type] || []), ...customTabConfig];
};

const getEntityLevelConfig = () => pluginConfiguration.entityLevelConfig?.flatMap(tab => tab?.structure || []) || [];

module.exports = {
	initPluginConfiguration,
	getCustomProperties,
	getFieldLevelConfig,
	getEntityLevelConfig,
};
