const { dependencies } = require('../../shared/appDependencies');
const { isNamedType } = require('../../shared/typeHelper');
const getFieldAttributes = require('./getFieldAttributes');
const { getNamespace, getName, EMPTY_NAMESPACE } = require('./generalHelper');
const { addDefinition, resolveRootReference, getDefinitions, filterUnusedDefinitions, updateRefs } = require('./referencesHelper');
const { getEntityLevelConfig, getFieldLevelConfig, getCustomProperties } = require('../../shared/customProperties');

const DEFAULT_FIELD_NAME = 'New_field';
const PRIMITIVE_TYPES = ['string', 'bytes', 'boolean', 'null', 'enum', 'fixed', 'int', 'long', 'float', 'double'];
const NUMERIC_TYPES = ['int', 'long', 'float', 'double'];

let _;
let collectionReferences = [];

const convertToJsonSchemas = avroSchema => {
	_ = dependencies.lodash;

	collectionReferences = avroSchema.references || [];
	const convertedSchema = convertSchema({ schema: avroSchema });
	const jsonSchemas = _.isArray(convertedSchema.type) ? convertedSchema.type : [ convertedSchema ];

	return jsonSchemas.map((schema, index) => {
		const relatedAvroSchema = _.isArray(avroSchema) ? avroSchema[index] : avroSchema;
		const customProperties = getCustomProperties(getEntityLevelConfig(), relatedAvroSchema);

		return _.flow([
			resolveRootReference,
			setSchemaRootAttributes(customProperties),
			filterUnusedDefinitions,
			updateRefs,
			setSchemaName,
		])(schema);
	});
};

const convertSchema = ({ schema, namespace = EMPTY_NAMESPACE, avroFieldAttributes = {} }) => {
	const fieldAttributes = getFieldAttributes({ attributes: avroFieldAttributes });
	if (_.isArray(schema)) {
		return convertUnion(namespace, schema);
	}

	if (!_.isObject(schema) && !_.isString(schema)) {
		return;
	}

	const type = _.isString(schema) ? schema : schema.type;
	const attributes =  setDefaultValue(_.isString(schema) ? {} : schema, fieldAttributes?.default);
	const field = convertType(namespace, type, getFieldAttributes({ attributes, type }));

	if (!isNamedType(type)) {
		return field;
	}

	const definition = {
		...field,
		...getCustomProperties(getFieldLevelConfig(field.type), avroFieldAttributes),
	};

	return addDefinition(attributes.namespace || namespace, setDefaultValue(definition, fieldAttributes?.default));
};

const setDefaultValue = (properties, defaultValue) => {
	return { ...properties, ...(defaultValue && { default: properties.default || defaultValue })};
};

const convertType = (parentNamespace, type, attributes) => {
	const namespace = attributes.namespace || parentNamespace;

	if (_.isArray(type)) {
		return convertUnion(namespace, type);
	}
	if (isNumericType(type)) {
		return convertNumeric(type, attributes);
	}

	if (type === 'enum') {
		return convertEnum(attributes);
	}

	if (isPrimitiveType(type)) {
		return convertPrimitive(type, attributes);
	}

	switch(type) {
		case 'fixed':
			return convertFixed(attributes);
		case 'map':
			return convertMap(namespace, attributes);
		case 'record':
			return convertRecord(namespace, attributes);
		case 'array':
			return convertArray(namespace, attributes);
		default:
			return convertUserDefinedType(namespace, type, attributes);
	}
};

const convertUnion = (namespace, types) => {
	if (types.length === 1) {
		return convertSchema({ schema: _.first(types), namespace });
	}

	return { type: types.map(schema => convertSchema({ schema, namespace })) };
};

const convertNumeric = (type, attributes) => ({ ...attributes, type: 'number', mode: type });

const convertEnum = attributes => ({
	..._.omit(attributes, 'default'),
	...( attributes.default && { symbolDefault: attributes.default }),
	type: 'enum',
});

const convertPrimitive = (type, attributes) => ({ ...attributes, type });

const convertMap = (namespace, attributes) => {
	const valuesSchema = handleMultipleFields([{ ...convertSchema({ schema: attributes.values, namespace }), name: 'schema' }]);

	return {
		..._.omit(attributes, 'values'),
		type: 'map',
		subtype: getMapSubtype(attributes.values),
		properties: convertArrayToJsonSchemaObject(valuesSchema),
		required: getRequired(valuesSchema),
	};
};

const convertFixed = attributes => ({ ...attributes, type: 'fixed' });

const convertRecord = (namespace, attributes) => {
	const fields = handleMultipleFields((attributes.fields || []).map(convertField(namespace)));

	return {
		..._.omit(attributes, 'fields'),
		type: 'record',
		properties: convertArrayToJsonSchemaObject(fields),
		required: getRequired(fields),
	};
};

const convertField = namespace => field => {
	const fieldTypeProperties = convertSchema({ schema: field.type, namespace, avroFieldAttributes: field });
	const type = _.isArray(fieldTypeProperties.type) ?
		fieldTypeProperties.type.map(({ type }) => type) : fieldTypeProperties.type;
	const customProperties = getCustomProperties(getFieldLevelConfig(type), field);

	return {
		...getFieldAttributes({ attributes: field }),
		...customProperties,
		...fieldTypeProperties,
		name: field.name,
	};
};

const convertArray = (namespace, attributes) => {
	const items = convertSchema({ schema: attributes.items, namespace }) || [];
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

const convertUserDefinedType = (namespace, type, attributes) => {
	const name = getName({ name: type });
	const ref = collectionReferences.map(reference => reference.name === name) ? `#collection/definitions/${name}` : type;

	return {
		...attributes,
		$ref: ref,
		definitionName: name,
		name: name,
		namespace: getNamespace({ name: type, namespace }),
	};
};

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

	return { ...field, ...multipleField, name: field.name };
};

const setSchemaRootAttributes = customProperties => schema => ({
	...schema,
	type: 'object',
	$schema: 'http://json-schema.org/draft-04/schema#',
	definitions: getDefinitions(),
	...customProperties,
});

const setSchemaName = schema => _.omit({ ...schema, title: getName(schema) }, ['namespace', 'name']);

const getOneOf = field => ({
	...field,
	type: 'choice',
	choice: 'oneOf',
	items: field.type.map(typeData => ({
		type: 'record',
		subschema: true,
		properties: { [field.name || DEFAULT_FIELD_NAME]: _.omit(typeData, 'name') },
	})),
});

const getMapSubtype = values => _.isString(values) ? `map<${values}>` : ''
const getRequired = properties => properties.filter(isRequired).map(field => field.name).filter(Boolean);
const isRequired = field => _.isUndefined(field.default);
const isPrimitiveType = type => PRIMITIVE_TYPES.includes(type);
const isNumericType = type => NUMERIC_TYPES.includes(type);
const hasComplexType = schemas => schemas.some(schema => !isPrimitiveType(schema.mode || schema.type));
const convertArrayToJsonSchemaObject = array => array.reduce((object, item) => ({
	...object,
	[item.name || DEFAULT_FIELD_NAME]: item,
}), {});

module.exports = convertToJsonSchemas;
