const { AVRO_TYPES } = require("./constants");

let pluginConfiguration = {};
let logger = {};

const initPluginConfiguration = (config, appLogger) => {
    logger = appLogger;
    pluginConfiguration = config || {};

    const customPropertiesConfigMap = getCustomPropertiesConfigMap();
    if (!Object.keys(customPropertiesConfigMap).length) {
        return;
    }

    logger?.log('info', `Plugin custom properties:\n${JSON.stringify(customPropertiesConfigMap, null, 4)}`, 'Custom Properties detected');
};

const getCustomPropertiesConfigMap = () => {
    const entityCustomProperties = getCustomProperties(getEntityLevelConfig());

    return AVRO_TYPES.reduce((typeToCustomProperties, type) => {
        const customProperties = getCustomProperties(getFieldLevelConfig(type));
        if (!customProperties.length) {
            return typeToCustomProperties;
        }

        return {
            ...typeToCustomProperties,
            [type]: customProperties,
        };
    }, {
        ...(entityCustomProperties.length && { entity: entityCustomProperties }),
    });
};

const getCustomProperties = config => config.filter(property => property.includeInScript);

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
	return filterConfigByDependency(getCustomProperties(config), attributes).map(property => property.fieldKeyword);
};

const getFieldLevelConfig = type => pluginConfiguration.fieldLevelConfig?.structure?.[type] || [];

const getEntityLevelConfig = () => pluginConfiguration.entityLevelConfig?.[0]?.structure || [];


module.exports = {
	initPluginConfiguration,
    getCustomPropertiesKeywords,
    getFieldLevelConfig,
    getEntityLevelConfig,
};
