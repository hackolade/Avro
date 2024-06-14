const { dependencies } = require('../../shared/appDependencies');
const { META_VALUES_KEY_MAP } = require('../../shared/constants');
const { isNamedType, filterAttributes, isMetaProperty } = require('../../shared/typeHelper');
const { getNamespace, getName } = require('./generalHelper');

let _;

const getFieldAttributes = ({ attributes = {}, type = '' }) => {
	_ = dependencies.lodash;

	let fieldAttributes = filterAttributes(attributes, type);
	fieldAttributes = setNamespace(fieldAttributes, type);
	fieldAttributes = setSubtype(fieldAttributes, type);
	fieldAttributes = setDescriptionFromDoc(fieldAttributes);
	fieldAttributes = convertDefaultValue(fieldAttributes);
	fieldAttributes = setDurationSize(fieldAttributes);
	fieldAttributes = addMetaProperties(fieldAttributes);

	return fieldAttributes;
};

const setNamespace = (properties, type) => {
	if (!isNamedType(type)) {
		return properties;
	}

	return {
		...properties,
		name: getName(properties),
		namespace: getNamespace(properties),
	};
};

const setSubtype = (properties, type) => {
	if (!properties.logicalType || !['fixed', 'bytes'].includes(type)) {
		return properties;
	}

	return { ...properties, subtype: properties.logicalType };
};

const setDescriptionFromDoc = properties => {
	if (!_.has(properties, 'doc')) {
		return properties;
	}

	return {
		..._.omit(properties, 'doc'),
		description: properties.doc,
	};
};

const convertDefaultValue = properties => {
	if (!_.isBoolean(properties.default)) {
		return properties;
	}

	return {
		...properties,
		default: properties.default.toString(),
	};
};

const setDurationSize = properties => {
	if (!_.has(properties, 'size') || properties.logicalType !== 'duration') {
		return properties;
	}

	return {
		..._.omit(properties, 'size'),
		durationSize: properties.size,
	};
};

const addMetaProperties = properties =>
	Object.keys(properties).reduce((updatedProperties, key) => {
		if (!isMetaProperty(key)) {
			return {
				...updatedProperties,
				[key]: properties[key],
			};
		}
		const metaValueKey = _.get(META_VALUES_KEY_MAP, key, 'metaValue');

		return {
			...updatedProperties,
			metaProps: [
				...(updatedProperties.metaProps || []),
				{
					metaKey: key,
					[metaValueKey]: properties[key],
				},
			],
		};
	}, {});

module.exports = getFieldAttributes;
