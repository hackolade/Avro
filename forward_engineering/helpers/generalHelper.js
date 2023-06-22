const { dependencies } = require('../../shared/appDependencies');

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

const compareSchemasByStructure = (schema1, schema2) => {
	const propertiesToCompare = ['type', 'name', 'fields', 'items', 'values', 'logicalType', 'precision', 'scale', 'size'];

	return _.isEqualWith(schema1, schema2, (schema1, schema2, key) => {
		if (
			!_.isUndefined(key) && // if key is undefined, one of the objects is empty
			!_.isNumber(key) && // if key is number, it's an array index
			!propertiesToCompare.includes(key)
		) { 
			return true;
		}

		if (key === 'fields') {
		/*
			we don't care much about the exact structure of nested fields,
			similarity is enough to detect should we
			replace schemas by the same reference or rise a warning in validator 
		*/

			return (
				_.isArray(schema1) && _.isArray(schema2) && 
				schema1.length === schema2.length &&
				_.isEqual(schema1.map(schema => schema.name), schema2.map(schema => schema.name))
			);
		}
		//if nothing is returned, _.isEqualWith will compare values by default
	});
};

module.exports = {
	parseJson,
	reorderAttributes,
	filterMultipleTypes,
	prepareName,
	simplifySchema,
	getDefaultName,
	convertName,
	compareSchemasByStructure,
};
