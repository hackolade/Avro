const { dependencies } = require('../../shared/appDependencies');
const { filterAttributes, isNamedType } = require('../../shared/typeHelper');
const { getUdtItem, convertSchemaToReference, addDefinitions } = require('./udtHelper');
const { reorderAttributes, filterMultipleTypes, prepareName, simplifySchema, getDefaultName, convertName, compareSchemasByStructure, } = require('./generalHelper');
const convertChoicesToProperties = require('./convertChoicesToProperties');
const { GENERAL_ATTRIBUTES, META_VALUES_KEY_MAP } = require('../../shared/constants');
const { getFieldLevelConfig, getCustomProperties } = require('../../shared/customProperties');
const getTypeFromReference = require('./getTypeFromReference');

const DEFAULT_TYPE = 'string';

let _;

const convertSchema = schema => {
	_ = dependencies.lodash;

	if (isBareUnionSchema(schema)) {
		return convertBareUnionSchema(schema)
	}

	schema = prepareSchema(schema);

	schema = convertDescriptionToDoc(schema);
	schema = convertDefault(schema);
	schema = convertLogicalType(schema);
	schema = convertSize(schema);
	schema = handleRequired(schema);
	schema = convertProperties(schema);
	schema = convertName(schema);
	schema = convertMetaProperties(schema);

	schema = convertType(schema);
	schema = filterSchemaAttributes(schema);
	schema = reorderAttributes(schema);
	schema = simplifySchema(schema);

	return schema;
};

const prepareSchema = schema => {
	const typeSchema =  {
		...convertChoicesToProperties(schema),
		type: !schema.type || (schema.$ref && !schema.choice) ? getTypeFromReference(schema) : getAvroType(schema.type),
	};

	if (!typeSchema.nullable) {
		return _.omit(typeSchema, 'nullable');
	}

	return {
		..._.omit(typeSchema, ['nullable', '$ref']),
		default: null,
		type: [
			'null',
			typeSchema.type
		],
	};
};

const isBareUnionSchema = (schema) => {
	if (!schema?.oneOf?.length || Object.values(schema?.properties ?? {}).length) {
		return false
	}

	return schema.oneOf.every(option => option.$ref)
}
const convertBareUnionSchema = (schema) => schema.oneOf.map(({confluentSubjectName, parentBucketName, name}) => confluentSubjectName ?? `${parentBucketName || schema.bucketName}.${name}`)

const getAvroType = type => {
	if (type === 'object') {
		return 'record';
	}

	return type;
};

const convertDescriptionToDoc = schema => {
	const description = schema.description;
	if (!description) {
		return schema;
	}

	return {
		...schema,
		doc: description,
	};
};

const convertDefault = schema => {
	const defaultValue = getDefault(schema);
	if (_.isUndefined(defaultValue)) {
		return schema;
	}

	return {
		...schema,
		default: defaultValue
	};
};

const convertLogicalType = schema => {
	const logicalType = schema.logicalType || schema.subtype;

	if (!logicalType) {
		return schema;
	}

	return {
		...schema,
		logicalType,
	};
};

const convertSize = schema => {
	if (schema.durationSize && schema.logicalType === 'duration') {
		return {
			...schema,
			size: schema.durationSize,
		};
	}

	return schema;
};

const handleRequired = schema => {
	if (!_.isArray(schema.required) || !schema.properties) {
		return schema;
	}

	return {
		...schema,
		properties: Object.keys(schema.properties).reduce((result, key) => {
			const property = schema.properties[key];
			if (!schema.required.includes(key) || property.nullable) {
				return { ...result, [key]: property };
			}

			return {
				...result,
				[key]: _.omit(property, 'default'),
			};
		}, {}),
	};
};

const convertProperties = schema => {
	if (!schema.properties) {
		return schema;
	};

	const propertiesKey = schema.type === 'map' ? 'values' : 'fields';

	return {
		...schema,
		[propertiesKey]: schema.properties
	};
};

const convertMetaProperties = schema => {
	if (_.isArray(schema.type) || schema.type === 'null') {
		return schema;
	}

	return {
		...schema,
		...getMetaProperties(schema.metaProps || []),
	};
};

const convertType = schema => {
	if (_.isArray(schema.type)) {
		return convertMultiple(schema);
	}

	switch(schema.type) {
		case 'string':
		case 'boolean':
		case 'null':
			return convertPrimitive(schema);
		case 'number':
			return convertNumber(schema);
		case 'bytes':
			return convertBytes(schema);
		case 'map':
			return convertMap(schema);
		case 'array':
			return convertArray(schema);
		case 'fixed':
			return convertFixed(schema);
		case 'enum':
			return convertEnum(schema);
		case 'record':
			return convertRecord(schema);
		default: // type is reference
			return schema;
	}
};

const filterSchemaAttributes = schema => filterAttributes(schema, schema.type);

const convertMultiple = schema => {
	const type = filterMultipleTypes(schema.type.map(type => {
		if(_.isString(type)) {
			const typeSchema = convertSchema({ ...schema, type });
			if (_.isString(typeSchema)) {
				return typeSchema;
			}

			return simplifySchema({ ..._.omit(typeSchema, GENERAL_ATTRIBUTES), type: typeSchema.type });
		}

		const fieldType = type.type || getTypeFromReference(type) || DEFAULT_TYPE;
		const typeAttributes = _.omit({ ...schema, ...type }, GENERAL_ATTRIBUTES);

		return convertSchema({
			...typeAttributes,
			type: fieldType,
		});
	}));

	let union = filterSchemaAttributes(_.omit(schema, 'type'));
	union = reorderAttributes(union);
	union = simplifySchema(union);

	return { ...union, type };
};

const convertBytes = schema => {
	return {
		...schema,
		...getLogicalTypeProperties(schema),
	};
};

const getLogicalTypeProperties = schema => {
	const logicalType = schema.logicalType;
	if (!logicalType) {
		return {};
	}

	switch (logicalType) {
		case 'decimal':
			return {
				logicalType,
				...(schema.precision && { precision: schema.precision }),
				...(schema.scale && { scale: schema.scale }),
			};
		default: 
			return { logicalType };
	};
};


const convertMap = schema => {
	return {
		...schema,
		values: schema.values ? getValuesSchema(schema.values) : DEFAULT_TYPE,
	};
};

const getValuesSchema = properties => {
	const schemaName = _.first(Object.keys(properties));

	return convertSchema(properties?.[schemaName] || {});
};

const convertFixed = schema => {
	return convertNamedType({
		...schema,
		name: schema.name || getDefaultName(),
		size: _.isUndefined(schema.size) ? 16 : schema.size,
		...getLogicalTypeProperties(schema),
	});
};

const convertArray = schema => {
	if (_.isArray(schema.items)) {
		const items = getUniqueItemsInArray(schema.items.map(item => convertSchema(item)));
		if (items.length === 1) {
			return {
				...schema,
				items: _.first(items),
			};
		}

		return {
			...schema,
			items,
		};
	}

	return {
		...schema,
		items: convertSchema(schema.items || { type: DEFAULT_TYPE }),
	};
};

const handleField = (name, field) => {
	const { description, refDescription, default: defaultValue, order, aliases, ...schema } = field;
	const typeSchema = convertSchema(schema);
	const udt = getUdtItem(typeSchema);
	const customProperties = udt?.customProperties || getCustomProperties(getFieldLevelConfig(schema.type), schema);

	return resolveFieldDefaultValue({
		name: prepareName(name),
		type: _.isArray(typeSchema.type) ? typeSchema.type : typeSchema,
		default: !_.isUndefined(defaultValue) ? defaultValue : typeSchema?.default,
		doc: field.$ref ? refDescription : description,
		order,
		aliases,
		...customProperties,
	}, typeSchema);
};

const resolveFieldDefaultValue = (field, type) => {
	let udtItem = _.isString(type) && getUdtItem(type);

	if (!udtItem || !isNamedType(udtItem.schema.type)) {
		return field;
	}

	const defaultValue = field.default || udtItem.schema.default;

	return {
		...field,
		default: defaultValue,
	};
};

const convertRecord = schema => {
	return convertNamedType({
		...schema,
		name: schema.name || getDefaultName(),
		fields: Object.keys(schema.fields || {}).map(name => handleField(name, schema.fields[name])),
	});
};

/**
 * Handler for named types (record, enum, fixed).
 * If this type is already defined, compare it with existing definition and return reference to it if they are equal.
 * If this type is not defined adds it to definitions.
 * 
 * 
 * @param {Object} schema 
 * @param {Object.<string, string>} [schemaTypeKeysMap] key map for properties on the schema type level which may have
 * collisions with the field level. For example: 
 * 
 * record field with enum type schema may have default that provides default value for this field
 * and also default on the type schema level that provides default value from symbols list
 * {
 * 	"name": "enumField",
 * 	"default": "defaultValue", // field default
 * 	"type": { // type schema
 *    "type": "enum",
 * 	  "name": "enumType",
 * 	  "default": "symbol1", // type schema default
 * 	  "symbols": ["symbol1", "symbol2"]
 * 	}
 * }
 * 
 * we use property named "symbolDefault" to distinguish this schema type default from field default 
 * 
 * example of usage: { symbolDefault: 'default'}
 * key of this property is our custom property name on the schema type level, value is the Avro name of this property
 * @returns {Object}
 */
const convertNamedType = (schema, schemaTypeKeysMap = {}) => {
	const name = schema.name;
	const schemaFromUdt = getUdtItem(name);
	const isAlreadyDefined = schemaFromUdt && !schemaFromUdt.isCollectionReference;
	const schemaTypeSpecificKeys = Object.keys(schemaTypeKeysMap);

	if (
		isAlreadyDefined &&
		compareSchemasByStructure(schema, schemaFromUdt.schema) 
		// if schemas are not equal, model is not valid for Avro. There will be a validation error
	) {
		return convertSchemaToReference(schema);
	}

	if (!schemaFromUdt) {
		addDefinitions({ [name]:  {
			schema: { ...filterSchemaAttributes(schema), ..._.pick(schema, schemaTypeSpecificKeys) },
			customProperties: getCustomProperties(getFieldLevelConfig(schema.type)),
			used: true,
		}});
	}

	return {
		..._.omit(schema, schemaTypeSpecificKeys),
		...schemaTypeSpecificKeys.reduce((props, key) => {
			return { ...props, [schemaTypeKeysMap[key]]: schema[key] };
		}, {}),
	};
};

const convertPrimitive = schema => {
	return schema;
};

const convertEnum = schema => {
	return convertNamedType({ ...schema, name: schema.name || getDefaultName() }, { symbolDefault: 'default' });
};

const convertNumber = schema => {
	return {
		...schema,
		type: schema.mode || 'int',
	}; 
};

const getUniqueItemsInArray = (items) => {
	return _.uniqWith(items, (item1, item2) => {
		item1 = normalizeSchema(item1);
		item2 = normalizeSchema(item2);
		if (isNamedType(item1.type) && isNamedType(item2.type)) {
			return item1.name === item2.name;
		}

		return item1.type === item2.type;
	});
};

const normalizeSchema = schema => _.isString(schema) ? { type: schema } : schema;

const getDefault = schema => {
	const defaultTypeSchema = _.isArray(schema.type) ? _.first(schema.type) : schema.type;
	const type = _.isString(defaultTypeSchema) ? defaultTypeSchema : defaultTypeSchema?.type;
	const value = _.isString(defaultTypeSchema) ? schema.default : defaultTypeSchema?.default;

	if (type === 'null' && value === 'null') {
		return null;
	}

	if (type === 'number' && !isNaN(value)) {
		return Number(value);
	}

	return value;
};

const getMetaProperties = (metaProperties) => {
	return metaProperties.reduce((props, property) => {
		const metaValueKey = _.get(META_VALUES_KEY_MAP, property.metaKey, 'metaValue');

		return { ...props, [property.metaKey]: property[metaValueKey] };
	}, {});
};

module.exports = convertSchema;