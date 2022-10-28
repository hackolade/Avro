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

module.exports = {
	parseJson,
	reorderAttributes,
	filterMultipleTypes,
	prepareName,
	simplifySchema,
	getDefaultName,
};
