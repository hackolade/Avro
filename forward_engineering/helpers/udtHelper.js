const { dependencies } = require('../../shared/appDependencies');
const { isNamedType, filterAttributes } = require('../../shared/typeHelper');
const { AVRO_TYPES } = require('../../shared/constants');
const { reorderAttributes, simplifySchema } = require('./generalHelper');
const mapAvroSchema = require('./mapAvroSchema');

let _;
let udt = {};

const getUdtItem = type => {
	_ = dependencies.lodash;

	return _.clone(udt[type]);
};

const resolveUdt = avroSchema => {
	_ = dependencies.lodash;

	return mapAvroSchema(avroSchema, resolveSchemaUdt);
};

const useUdt = type => {
	if (!udt[type]) {
		return;
	}

	udt[type].used = true
};

const prepareTypeFromUDT = (typeFromUdt) => {
	if (_.isString(typeFromUdt)) {
		return { type: typeFromUdt };
	}

	return { ...typeFromUdt };
};

const isUdtUsed = type => {
	const udtItem = getUdtItem(type);

	return !udtItem || udtItem.used;
};

const isDefinitionTypeValidForAvroDefinition = definition => {
	if (_.isString(definition)) {
		return isNamedType(definition);
	} else if (_.isArray(definition)) {
		return definition.some(isDefinitionTypeValidForAvroDefinition);
	} else {
		return isNamedType(definition?.type);
	}
}

const resolveSchemaUdt = schema => {
	const type = _.isString(schema) ? schema : schema.type;
	if (isNativeType(type)) {
		return schema;
	}

	if (_.isString(schema)) {
		const typeFromUdt = getTypeFromUdt(schema);
	
		return typeFromUdt;
	}

	if (_.isArray(type)) {
		return { ...schema, type: type.map(resolveSchemaUdt) };
	}

	const typeFromUdt = getTypeFromUdt(type);
	if (_.isArray(_.get(typeFromUdt, 'type'))) {
		return { ...schema, ...typeFromUdt, name: schema.name };
	}

	return { ...schema, ...prepareTypeFromUDT(typeFromUdt), name: schema.name };
};

const isNativeType = type => {
	const udtItem = getUdtItem(type);

	return !udtItem && _.includes(AVRO_TYPES, type);
};

const getTypeFromUdt = type => {
	if (isUdtUsed(type)) {
	    return getTypeWithNamespace(type);
	}

	let udtItem = getUdtItem(type);

	if (isDefinitionTypeValidForAvroDefinition(udtItem)) {
		useUdt(type);
	} else {
		udtItem = mapAvroSchema(udtItem, convertNamedTypesToReferences);
	}

	if (_.isString(udtItem.type)) {
		return udtItem;
	}

	return resolveSchemaUdt(udtItem);
};

const getTypeWithNamespace = type => {
	const udtItem = getUdtItem(type);

	if (!udtItem) {
		return type;
	}

	if (!udtItem.namespace) {
		return type;
	}

	return udtItem.namespace + '.' + type;
};

const convertNamedTypesToReferences = schema => {
	if (!isNamedType(schema.type)) {
		return schema;
	}

	if (!udt[schema.name] || !isUdtUsed(schema.name)) {
		udt[schema.name] = schema;
	}

	return simplifySchema(convertSchemaToReference(schema));
};

const convertSchemaToReference = schema => {
	_ = dependencies.lodash;

	const referenceAttributes = filterAttributes()(_.omit(schema, 'type'));

	return reorderAttributes({ ...referenceAttributes, type: schema.name });
};

const addDefinitions = definitions => {
	udt = { ...udt, ...definitions };
};

const resetDefinitionsUsage = () => {
	_ = dependencies.lodash;

	udt = Object.keys(udt || {}).reduce((updatedUdt, key) => {
		const definition = udt[key];

		return { ...updatedUdt, [key]: _.isString(definition) ? definition : _.omit(definition, 'used') };
	}, {});
};

module.exports = {
   resolveUdt,
   getUdtItem,
   addDefinitions,
   convertSchemaToReference,
   resetDefinitionsUsage,
};
