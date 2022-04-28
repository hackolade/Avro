
const { dependencies } = require('../appDependencies');
const { getNamespace, getName } = require('./generalHelper');

const META_PROPERTIES = ['avro.java.string', 'java-element', 'java-element-class', 'java-class', 'java-key-class'];
const META_VALUES_KEY_MAP = {
	'avro.java.string': 'metaValueString',
	'java-element': 'metaValueElement',
	'java-element-class': 'metaValueElementClass',
	'java-class': 'metaValueClass',
	'java-key-class': 'metaValueKeyClass'
};
const NAMED_TYPES = ['record', 'fixed', 'enum'];
const GENERAL_ATTRIBUTES = ['doc', 'aliases', 'order', 'pattern', 'default'];
const TYPE_SPECIFIC_ATTRIBUTES = {
	enum: ['name', 'namespace', 'symbols'],
	array: ['items'],
	map: ['values'],
	record: ['name', 'namespace', 'fields'],
	fixed: ['name', 'namespace', 'size', 'logicalType'],
	string: ['logicalType'],
	bytes: ['logicalType'],
	int: ['logicalType'],
	long: ['logicalType'],
};
const DECIMAL_ATTRIBUTES = ['precision', 'scale'];

let _;

const getFieldAttributes = (attributes = {}, type = '') => {
	_ = dependencies.lodash;

    return _.flow([
        filterAttributes(type),
        setNamespace(type),
        setSubtype(type),
        setDescriptionFromDoc,
        convertDefaultValue,
        setDurationSize,
        addMetaProperties,
    ])(attributes);
};

const filterAttributes = type => attributes => _.pick(attributes, [
	...(TYPE_SPECIFIC_ATTRIBUTES[type] || []),
	...GENERAL_ATTRIBUTES,
	...getLogicalTypeProperties(attributes.logicalType),
	...META_PROPERTIES
]);

const getLogicalTypeProperties = logicalType => logicalType === 'decimal' ? DECIMAL_ATTRIBUTES : [];

const setNamespace = type => properties => {
	if (!isNamedType(type)) {
		return properties;
	}

	return {
		...properties,
		name: getName(properties),
		namespace: getNamespace(properties),
	};
};

const setSubtype = type => properties => {
	if (!properties.logicalType || !['fixed', 'bytes'].includes(type)) {
		return properties
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

const addMetaProperties = properties => Object.keys(properties).reduce((updatedProperties, key) => {
	if (!isMetaProperty(key)) {
		return {
			...updatedProperties,
			[key]: properties[key],
		};
	}
	const metaValueKey = _.get(META_VALUES_KEY_MAP, key, 'metaValue');

	return {
		...updatedProperties,
		metaProps: [...(updatedProperties.metaProps || []), {
			metaKey: key, [metaValueKey]: properties[key]
		}],
	};
}, {});

const isNamedType = type => NAMED_TYPES.includes(type);
const isMetaProperty = type => META_PROPERTIES.includes(type)

module.exports = getFieldAttributes;