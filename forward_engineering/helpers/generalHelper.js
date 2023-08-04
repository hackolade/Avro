const { dependencies } = require('../../shared/appDependencies');
const { filterAttributes } = require('../../shared/typeHelper');

let _;
let nameIndex = 0;

const DEFAULT_NAME = 'New_field';

const parseJson = str => {
	try {
		return JSON.parse(str);
	} catch (e) {
		return {};
	}
};

const reorderAttributes = avroSchema => {
	_ = dependencies.lodash;

	return _.flow([
	    setPropertyAsFirst('type'),
	    setPropertyAsFirst('doc'),
	    setPropertyAsFirst('namespace'),
	    setPropertyAsFirst('name'),
	])(avroSchema);
};

const setPropertyAsFirst = key => avroSchema => {
	const objKeys = Object.keys(avroSchema);
	if (!objKeys.includes(key)) {
		return avroSchema;
	}

	const reorderedKeys = [key, ...objKeys.filter(item => item !== key)];

	return reorderedKeys.reduce((avroSchema, key) => ({
		..._.omit(avroSchema, key), [key]: avroSchema[key]
	}), avroSchema);
};

const filterMultipleTypes = schemaTypes => {
	_ = dependencies.lodash;

	const types = _.uniqBy(schemaTypes, type => type?.type || type);
	if (types.length === 1) {
		return _.first(types);
	}

	return types;
};

const prepareName = name => {
	const VALID_FULL_NAME_REGEX = /[^A-Za-z0-9_]/g;
	const VALID_FIRST_NAME_LETTER_REGEX = /^[0-9]/;

	return (name || '')
		.replace(VALID_FULL_NAME_REGEX, '_')
		.replace(VALID_FIRST_NAME_LETTER_REGEX, '_');
};

const prepareNamespace = namespace => {
	if (!namespace) {
		return '';
	}

	return namespace.split('.').map(prepareName).join('.');
};

const simplifySchema = schema => {
	const filteredSchema = Object.keys(schema).reduce((filteredSchema, key) => {
		if (_.isUndefined(schema[key])) {
			return filteredSchema;
		}

		return {
			...filteredSchema,
			[key]: schema[key],
		};
	}, {});

	if (Object.keys(filteredSchema).length === 1 && filteredSchema.type) {
		return filteredSchema.type;
	}

	return filteredSchema;
};

const getDefaultName = () => {
	const defaultName = nameIndex ? `${DEFAULT_NAME}_${nameIndex++}` : DEFAULT_NAME;
	nameIndex++;

	return defaultName;
};

const convertName = schema => {
	_ = dependencies.lodash;

	const nameProperties = ['typeName', 'code', 'name', 'displayName'];
	const nameKey = nameProperties.find(key => schema[key]);
	if (!nameKey) {
		return schema;
	}

	return { ..._.omit(schema, nameProperties), name: prepareName(schema[nameKey])};
};

/**
 * Compares two schemas by their structure. Equality is determined by comparing critical type properties and fields names (for records).
 * 
 * @param {Object} schema1
 * @param {Object} schema2
 * @returns {Boolean}
 */
const compareSchemasByStructure = (schema1, schema2) => {
	schema1 = filterAttributes(schema1, schema1.type);
	schema2 = filterAttributes(schema2, schema2.type);
	const scalarPropertiesToCompare = ['type', 'name', 'logicalType', 'precision', 'scale', 'size'];

	const isEqualByProperties = _.isEqual(_.pick(schema1, scalarPropertiesToCompare), _.pick(schema2, scalarPropertiesToCompare));
	if (!isEqualByProperties) {
		return false;
	}

	const hasStructure = schema1.fields || schema2.fields;

	if (!hasStructure) {
		return true;
	}

	return _.isEqual(_.map(schema1.fields, 'name'), _.map(schema2.fields, 'name'));
};

module.exports = {
	parseJson,
	reorderAttributes,
	filterMultipleTypes,
	prepareName,
	prepareNamespace,
	simplifySchema,
	getDefaultName,
	convertName,
	compareSchemasByStructure,
};
