const { dependencies } = require('../../shared/appDependencies');
const { isNamedType } = require('../../shared/typeHelper');
const getFieldAttributes = require('./getFieldAttributes');
const { getNamespace, getName, EMPTY_NAMESPACE } = require('./generalHelper');
const { addDefinition, resolveRootReference, getDefinitions, filterUnusedDefinitions, updateRefs, isBareUnionSchema} = require('./referencesHelper');
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
	const normalizedConvertedSchema = _.isArray(convertedSchema) ? convertedSchema : [ convertedSchema ]
	const jsonSchemas = _.isArray(convertedSchema.type) ? convertedSchema.type : normalizedConvertedSchema;

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
	const attributes = setDefaultValue(_.isString(schema) ? {} : schema, fieldAttributes?.default);
	const field = convertType(namespace, type, getFieldAttributes({ attributes, type }));

	/*Here schema which consists only of a union (simply, array of options) is handled and adapted for appropriate structure in the app**/
	if (isBareUnionSchema(schema, type)) {
		if (bareUnionSchemaIncludesOnlyReferences(schema)) {
			/** This handler takes care about union which options are all references */
			return convertBareUnionSchemaWithReferences(namespace, schema)
		} else if (bareUnionSchemaIncludesOnlyDefinitionRecords(schema)) {
			/** This handler takes care about union which options are all records defined inside the union itself */
			return convertBareUnionSchemaWithRecordDefinitions(namespace, schema)
		}
	}

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

const convertBareUnionSchemaWithReferences = (namespace, schema) => {
	const bareUnionSchemaType = 'record'
	const schemaFullNameComponents = (schema?.schemaTopic || schema?.confluentSubjectName || '').split('.')
	const [schemaName] = schemaFullNameComponents.slice(-1)
	const parsedSchemaNamespace = schemaFullNameComponents.slice(0, -1).join('.')
	
	const schemaNamespace = parsedSchemaNamespace || namespace || EMPTY_NAMESPACE
	const schemaWithoutUnionOptions = Object.fromEntries(Object.entries(schema).filter(([name, _]) => isNaN(parseInt(name))))
	const bareUnionSchemaUsedTypes = schema.references.map(({name}) => convertUserDefinedType(schemaNamespace, name, {}))

	const bareUnionSchema = {
		...schemaWithoutUnionOptions,
		name: schemaName,
		type: bareUnionSchemaType,
		namespace: schemaNamespace,
		fields: [
			{
				type: bareUnionSchemaUsedTypes
			}
		]
	}

	const attributes = getFieldAttributes({ attributes: bareUnionSchema, type: bareUnionSchemaType })
	const schemaFields = (attributes.fields || []).map(item => getBareUnionSchemaOneOf(item))

	const convertedBareUnionSchema = {
		..._.omit(attributes, 'fields'),
		type: bareUnionSchemaType,
		properties: convertArrayToJsonSchemaObject(schemaFields),
		required: getRequired(schemaFields),
	};

	return addDefinition(schemaNamespace, setDefaultValue(convertedBareUnionSchema, {}));
}

const convertBareUnionSchemaWithRecordDefinitions = (namespace, schema) => {
	const recordDefinitionType = 'record'
	const parsedSchemaNamespace = (schema?.schemaTopic || schema?.confluentSubjectName || '').split('.').slice(0, -1).join('.')
	
	const schemaNamespace = parsedSchemaNamespace ?? namespace
	const unionOptionsKeysInSchema = Object.keys(schema).filter(key => !isNaN(parseInt(key)))
	return unionOptionsKeysInSchema.map(key => {
		const attributes = getFieldAttributes({ attributes: schema[key], type: recordDefinitionType })
		const field = convertType(namespace, recordDefinitionType, getFieldAttributes({ attributes, type: recordDefinitionType }));

		return addDefinition(schemaNamespace, field);
	})
}

const isReferenceUnionOption = option => _.isString(option) && isCollectionReference(option)
const isRecordDefinitionUnionOption = option => _.isObject(option) && option.type === 'record'

const bareUnionSchemaIncludesOnlyReferences = schema => {
	const unionOptionsKeysInSchema = Object.keys(schema).filter(key => !isNaN(parseInt(key)))

	return unionOptionsKeysInSchema.every(optionKey => isReferenceUnionOption(schema[optionKey]))
}

const bareUnionSchemaIncludesOnlyDefinitionRecords = schema => {
	const unionOptionsKeysInSchema = Object.keys(schema).filter(key => !isNaN(parseInt(key)))

	return unionOptionsKeysInSchema.every(optionKey => isRecordDefinitionUnionOption(schema[optionKey]))
}

const convertUnion = (namespace, types) => {
	if (types.length === 1) {
		return convertSchema({ schema: _.first(types), namespace });
	}

	if (isNullableCollectionReference(types)) {
		const [_nullType, collectionReference] = types;
		const schema = convertSchema({ schema: collectionReference, namespace });

		return { ...schema, nullable: true };
	}

	return { type: types.map(schema => convertSchema({ schema, namespace })) }
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
	let fieldAttributes = getFieldAttributes({ attributes: field });
	fieldAttributes = fieldTypeProperties.nullable ?  _.omit(fieldAttributes, 'default') : fieldAttributes;

	return {
		..._.omit(fieldAttributes, 'type'),
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
	const hasNamespaceSpecified = (type || '').split('.').length > 1;
	const ref = (isCollectionReference(name) || hasNamespaceSpecified) ? `#collection/definitions/${name}` : type;

	return {
		...attributes,
		$ref: ref,
		definitionName: name,
		name: name,
		namespace: getNamespace({ name: type, namespace }),
	};
};

const isCollectionReference = name => _.isString(name)
	&& (!!collectionReferences.find(reference => reference.name === name) || (name || '').split('.').length > 1);
const isNullableCollectionReference = unionSchema => unionSchema[0] === 'null' && isCollectionReference(unionSchema[1]);

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

const getBareUnionSchemaOneOf = field => ({
	...field,
	type: 'choice',
	choice: 'oneOf',
	items: field.type.map(typeData => ({
		...typeData, 
		type: 'record',
		subschema: true,
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
