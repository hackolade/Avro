const { dependencies } = require('../appDependencies');
const getFieldAttributes = require('./getFieldAttributes');
const { getNamespace, getName, EMPTY_NAMESPACE } = require('./generalHelper');
const { addDefinition, resolveRootReference, getDefinitions, filterUnusedDefinitions, updateRefs } = require('./referencesHelper');

const DEFAULT_FIELD_NAME = 'New_field';
const PRIMITIVE_TYPES = ['string', 'bytes', 'boolean', 'null', 'enum', 'fixed', 'int', 'long', 'float', 'double'];
const NUMERIC_TYPES = ['int', 'long', 'float', 'double'];

let _;

const convertToJsonSchemas = avroSchema => {
	_ = dependencies.lodash;

	const convertedSchema = convertSchema(avroSchema);
	const jsonSchemas = _.isArray(convertedSchema.type) ? convertedSchema.type : [ convertedSchema ];

	return jsonSchemas.map(_.flow([
		resolveRootReference,
		setSchemaRootAttributes,
		filterUnusedDefinitions,
		updateRefs,
		setSchemaName,
	]));
};

const convertSchema = (schema, namespace = EMPTY_NAMESPACE) => {
	if (_.isString(schema)) {
		return convertType(namespace, schema, getFieldAttributes(schema));
	}
	if (_.isPlainObject(schema)) {
		return convertType(namespace, schema.type, getFieldAttributes(schema.type, schema));
	}
	if (_.isArray(schema)) {
		return convertUnion(namespace, schema);
	}
};

const convertType = (parentNamespace, type, attributes) => {
	const namespace = attributes.namespace || parentNamespace;

	if (_.isArray(type)) {
		return convertUnion(namespace, type);
	}
	if (isNumericType(type)) {
		return convertNumeric(type, attributes);
	}
	if (isPrimitiveType(type)) {
		return convertPrimitive(type, attributes);
	}

	switch(type) {
		case 'map':
			return convertMap(namespace, attributes);
		case 'enum':
			return addDefinition(namespace, convertEnum(attributes));
		case 'fixed':
			return addDefinition(namespace, convertFixed(attributes));
		case 'record':
			return addDefinition(namespace, convertRecord(namespace, attributes));
		case 'array':
			return convertArray(namespace, attributes);
		default:
			return convertUserDefinedType(namespace, type, attributes);
	}
};

const convertUnion = (namespace, types) => {
	if (types.length === 1) {
		return convertSchema(_.first(types), namespace);
	}

	return { type: types.map(schema => convertSchema(schema, namespace)) };
};

const convertNumeric = (type, attributes) => ({ ...attributes, type: 'number', mode: type });
const convertPrimitive = (type, attributes) => ({ ...attributes, type });

const convertMap = (namespace, attributes) => {
	const valuesSchema = handleMultipleFields([{ ...convertSchema(attributes.values, namespace), name: 'schema' }]);

	return {
		..._.omit(attributes, 'values'),
		type: 'map',
		subtype: getMapSubtype(attributes.values),
		properties: convertArrayToJsonSchemaObject(valuesSchema),
		required: getRequired(valuesSchema),
	};
};

const convertEnum = attributes => ({ ...attributes, type: 'enum' });
const convertFixed = attributes => ({ ...attributes, type: 'fixed' });

const convertRecord = (namespace, attributes) => {
	const fields = handleMultipleFields(attributes.fields.map(convertField(namespace)));

	return {
		..._.omit(attributes, 'fields'),
		type: 'record',
		properties: convertArrayToJsonSchemaObject(fields),
		required: getRequired(fields),
	};
};

const convertField = namespace => field => ({
	...getFieldAttributes('', field),
	...convertSchema(field.type, namespace),
	name: field.name
});

const convertArray = (namespace, attributes) => {
	const items = convertSchema(attributes.items, namespace) || [];
	const multipleTypes = handleMultipleFields(_.isArray(items) ? items : [items]);
	const [ choices, fields ] = _.partition(multipleTypes, field => field.type === 'choice');

	return {
		...attributes,
		type: 'array',
		items: fields,
		properties: convertArrayToJsonSchemaObject(choices),
		required: getRequired(multipleTypes),
	};
};

const convertUserDefinedType = (namespace, type, attributes) => ({
	...attributes,
	$ref: type,
	definitionName: getName({ name: type }),
	name: getName({ name: type }),
	namespace: getNamespace({ name: type, namespace }),
});

const handleMultipleFields = items => items.map(item => {
	if (!_.isArray(item.type)) {
		return item;
	}

	return hasComplexType(item.type) ? getOneOf(item) : mergeMultipleFieldProperties(item);
});

const mergeMultipleFieldProperties = field => {
	const multipleField = field.type.reduce((multipleField, schema) => ({
		...multipleField,
		...schema,
		type: [ ...(multipleField.type || []), schema.type ],
	}), {});

	return { ...multipleField, name: field.name };
};

const setSchemaRootAttributes = schema => ({
	...schema,
	type: 'object',
	$schema: 'http://json-schema.org/draft-04/schema#',
	definitions: getDefinitions(),
});

const setSchemaName = schema => _.omit({ ...schema, title: getName(schema) }, ['namespace', 'name']);

const getOneOf = field => ({
	...field,
	type: 'choice',
	choice: 'oneOf',
	items: field.type.map(typeData => ({
		type: 'record',
		subschema: true,
		properties: { [field.name]: _.omit(typeData, 'name') },
	})),
});

const getMapSubtype = values => _.isString(values) ? `map<${values}>` : ''
const getRequired = properties => properties.filter(isRequired).map(field => field.name).filter(Boolean);
const isRequired = field => !field.default;
const isPrimitiveType = type => PRIMITIVE_TYPES.includes(type);
const isNumericType = type => NUMERIC_TYPES.includes(type);
const hasComplexType = schemas => schemas.some(schema => !isPrimitiveType(schema.mode || schema.type));
const convertArrayToJsonSchemaObject = array => array.reduce((object, item) => ({
	...object,
	[item.name || DEFAULT_FIELD_NAME]: item,
}), {});

module.exports = convertToJsonSchemas;
