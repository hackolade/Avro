const { dependencies } = require('../../shared/appDependencies');
const { isNamedType, filterAttributes } = require('../../shared/typeHelper');
const { AVRO_TYPES, SCRIPT_TYPES } = require('../../shared/constants');
const mapJsonSchema = require('../../shared/mapJsonSchema');
const { reorderAttributes, simplifySchema } = require('./generalHelper');
const mapAvroSchema = require('./mapAvroSchema');
const { getConfluentSubjectName } = require('./formatAvroSchemaByType');
const { prepareName } = require('./generalHelper');

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
		return reorderAttributes({
			...schema,
			...typeFromUdt,
			...(schema.name && { name: schema.name }),
			...(schema.doc && { doc: schema.doc }),
		});
	}

	return reorderAttributes({
		...schema,
		...prepareTypeFromUDT(typeFromUdt),
		...(schema.name && { name: schema.name }),
		...(schema.doc && { doc: schema.doc }),
	});
};

const isNativeType = type => {
	const udtItem = getUdtItem(type);

	return !udtItem && _.includes(AVRO_TYPES, type);
};

const getTypeFromUdt = type => {
	if (isUdtUsed(type)) {
	    return getTypeWithNamespace(type);
	}

	let udtItem = resolveSymbolDefaultValue(getUdtItem(type));

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

const resolveSymbolDefaultValue = udtItem => {
	if (udtItem.type !== 'enum') {
		return udtItem;
	}

	return {
		..._.omit(udtItem, 'symbolDefault'),
		default: udtItem.symbolDefault,
	};
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

const resolveCollectionReferences = (entities, scriptType) => {
	_ = dependencies.lodash;

	if (scriptType !== SCRIPT_TYPES.CONFLUENT_SCHEMA_REGISTRY) {
		return entities;
	}

	const entitiesIds = entities.map(entity => entity.jsonSchema.GUID);
	const entitiesWithReferences = entities.map(entity => {
		let references = [];
		const mapper = mapJsonSchema(field => {
			if (!field.ref || !entitiesIds.includes(field.ref)) {
				return field;
			}

			const definition = entities.find(entity => entity.jsonSchema.GUID === field.ref).jsonSchema;
			const definitionName = definition.code || definition.collectionName;

			const subject = getConfluentSubjectName({
				name: definitionName,
				namespace: definition.bucketName,
				schemaType: definition.schemaType,
				schemaTopic: definition.schemaTopic,
				schemaNameStrategy: definition.schemaNameStrategy,
				confluentSubjectName: definition.confluentSubjectName,
			});

			references = [...references, {
				name: definitionName,
				subject,
				version: getConfluentSchemaVersion(definition.confluentVersion),
			}];

			return {
				$ref: `#/definitions/${definition.code || definition.collectionName}`,
			};
		});

		return {
			...entity,
			jsonSchema: mapper(entity.jsonSchema),
			references,
		};
	});

	return topologicalSort(entitiesWithReferences);
};

const topologicalSort = allEntities => {
	let [entities, noDependenciesEntities] = _.partition(allEntities, entity => {
		const dependencies = (entity.references || []).map(reference => reference.name);

		return !_.isEmpty(dependencies);
	});
	const allNames = allEntities.map(getSchemaNameFromEntity);
	let sortedEntities = noDependenciesEntities;
	let sortedNames = sortedEntities.map(getSchemaNameFromEntity);

	while (entities.length > 1) {
		for (let i in entities) {
			const entity = entities[i];
			const dependencies = (entities[i].references || [])
				.map(reference => reference.name)
				.filter(dependency => allNames.includes(dependency));

			if (dependencies.every(dependency => sortedNames.includes(dependency))) {
				sortedEntities.push(entity);
				sortedNames.push(getSchemaNameFromEntity(entities[i]));
				entities.splice(i, 1);
			}
		}
	}

	if (sortedEntities.length !== allEntities.length) {
		return [...sortedEntities, ...allEntities.filter(entity => !sortedEntities.includes(entity))];
	}

	return sortedEntities;
};

const getSchemaNameFromEntity = entity => prepareName(entity.entityData?.code || entity.entityData?.name || entity.entityData?.collectionName);

const getConfluentSchemaVersion = version => {
	if (!version) {
		return 1;
	}

	if (isNaN(version)) {
		return version;
	}

	return Number(version);
};

module.exports = {
   resolveUdt,
   getUdtItem,
   addDefinitions,
   convertSchemaToReference,
   resetDefinitionsUsage,
   resolveCollectionReferences,
};
