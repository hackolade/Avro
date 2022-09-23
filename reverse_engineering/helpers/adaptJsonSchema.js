const { dependencies } = require('../appDependencies');
const mapJsonSchema = require('./mapJsonSchema');

const COMPLEX_PROPERTIES = ['patternProperties', 'properties', 'items', 'allOf', 'oneOf', 'anyOf', 'not'];

let _;

const adaptJsonSchema = jsonSchema => {
	_ = dependencies.lodash;

	return mapJsonSchema(_.flow([
		adaptType,
		populateDefaultNullValuesForMultiple,
		handleEmptyDefaultInProperties
	]))(adaptNames(jsonSchema))
};

const adaptJsonSchemaName = name => {
	_ = dependencies.lodash;

	return convertToValidAvroName(name);
};

const adaptType = field => {
	const type = field.type;

	if (_.isArray(type)) {
		return adaptMultiple(field);
	}

	if (type === 'string') {
		return handleStringFormat(field);
	}

	if (type === 'number') {
		return handleNumber(field);
	}

	if (type === 'integer' || type === 'int') {
		return handleInt(field);
	}

	return field;
};

const populateDefaultNullValuesForMultiple = field => {
	if (!_.isArray(field.type))	{
		return field;
	}
	if (_.first(field.type) !== 'null') {
		return field;
	}

	return { ...field, default: null };
};

const handleEmptyDefaultInProperties = field => {
	let required = _.get(field, 'required', []);

	if (!_.isPlainObject(field.properties)) {
		return field;
	}

	const isRoot = field.$schema && field.type === 'object';
	const propertiesKeys = Object.keys(field.properties);
	if (isRoot && propertiesKeys.length === 1 && isComplexType(field.properties[_.first(propertiesKeys)].type)) {
		return field;
	}

	const properties = propertiesKeys.reduce((properties, key) => {
		const propertyValue = field.properties[key];
		if (required.includes(key)) {
			return { ...properties, [key]: propertyValue };
		}

		const property = handleEmptyDefault(propertyValue);
		if (propertyValue === property || !_.isArray(property.type)) {
			return { ...properties, [key]: propertyValue };
		}

		required = required.filter(name => name !== key);
		const hasComplexType = property.type.find(isComplexType);

		if (!hasComplexType) {
			return { ...properties, [key]: property };
		}

		return {
			...properties,
			[key]: {
				..._.omit(property, [ ...COMPLEX_PROPERTIES, 'type' ]),
				oneOf: getOneOf(property),
			}
		};
	}, {});

	return {
		...field,
		properties,
		required,
	};
};

const getOneOf = property => property.type.map(type => {
	if (!isComplexType(type)) {
		return {
			..._.omit(property, COMPLEX_PROPERTIES),
			type
		}
	}

	return {
		..._.omit(property, type === 'array' ? ['patternProperties', 'properties'] : 'items'),
		type
	};
});

const handleDate = field => ({
	...field,
	type: 'number',
	mode: 'int',
	logicalType: 'date',
})

const handleTime = field => ({
	...field,
	type: 'number',
	mode: 'int',
	logicalType: 'time-millis',
});

const handleDateTime = field => ({
	...field,
	type: 'number',
	mode: 'long',
	logicalType: 'timestamp-millis',
});

const handleNumber = field => {
	if ((field.mode && field.mode !== 'decimal') || field.logicalType) {
		return field;
	}

	return {
		...field,
		type: 'bytes',
		subtype: 'decimal',
	};
};

const handleInt = field => ({
	...field,
	type: 'number',
	mode: 'int'
});

const handleStringFormat = field => {
	const { format, ...fieldData } = field;

	switch(format) {
		case 'date':
			return handleDate(fieldData);
		case 'time':
			return handleTime(fieldData);
		case 'date-time':
			return handleDateTime(fieldData);
		default:
			return field;
	};
};

const adaptMultiple = field => {
	const { fieldData, types } = field.type.reduce(({ fieldData, types }, type, index) => {
		const typeField = { ...fieldData, type };
		const updatedData = adaptType(typeField);
		types[index] = updatedData.type;

		return {
			fieldData: updatedData,
			types
		};
	}, { fieldData: field, types: field.type });

	const uniqTypes = _.uniq(types);
	if (uniqTypes.length === 1) {
		return fieldData;
	}

	return { ...fieldData, type: uniqTypes };
};

const handleEmptyDefault = field => {
	const hasDefault = !_.isUndefined(field.default) && field.default !== '';
	const isMultiple = _.isArray(field.type);
	const types = isMultiple ? field.type : [ field.type ];

	if (hasDefault || _.first(types) === 'null') {
		return field;
	}

	return {
		...field,
		default: null,
		type: _.uniq([ 'null', ...types ]),
	};
};

const isComplexType = type => ['object', 'record', 'array', 'map'].includes(type)

const adaptTitle = jsonSchema => {
	if (!jsonSchema.title) {
		return jsonSchema;
	}

	return {
		...jsonSchema,
		title: convertToValidAvroName(jsonSchema.title),
	};
};

const adaptRequiredNames = jsonSchema => {
	if (!_.isArray(jsonSchema.required)) {
		return jsonSchema;
	}

	return {
		...jsonSchema,
		required: jsonSchema.required.map(convertToValidAvroName),
	};
};

const adaptPropertiesNames = jsonSchema => {
	if (!_.isPlainObject(jsonSchema)) {
		return jsonSchema;
	}

	const propertiesKeys = [ 'properties', 'definitions', 'patternProperties' ];
	
	const adaptedSchema = adaptRequiredNames(jsonSchema);

	return propertiesKeys.reduce((schema, propertyKey) => {
		const properties = schema[propertyKey];
		if (_.isEmpty(properties)) {
			return schema;
		}

		const adaptedProperties = Object.keys(properties).reduce((adaptedProperties, key) => {
			if (key === '$ref') {
				return {
					...adaptedProperties,
					[key]: convertReferenceName(properties[key]),
				};
			}

			return {
				...adaptedProperties,
				[convertToValidAvroName(key)]: adaptPropertiesNames(properties[key]),
			};
		}, {});

		return {
			...schema,
			[propertyKey]: adaptedProperties,
		};
	}, adaptedSchema);
};

const adaptNames = schema => _.flow([
	adaptTitle,
	adaptPropertiesNames
])(schema);

const convertReferenceName = ref => {
	if (!_.isString(ref)) {
		return ref;
	}

	const refNames = ref.split('/');
	const referenceName = _.last(refNames);
	const adaptedReferenceName = convertToValidAvroName(referenceName);

	return refNames.slice(0, -1).concat(adaptedReferenceName).join('/');
};

const convertToValidAvroName = name => {
	if (!_.isString(name)) {
		return name;
	}

	return name.replace(/[^A-Za-z0-9_]/g, '_');
};

module.exports = { adaptJsonSchema, adaptJsonSchemaName };
