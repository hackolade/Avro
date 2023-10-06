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
	if (!_.isString(type)) {
		return;
	}

	const definitionName = _.last(type.split('.'));

	return _.clone(udt[definitionName]);
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

	return !udtItem || udtItem.used || udtItem.isCollectionReference;
};

const isDefinitionTypeValidForAvroDefinition = definition => {
	if (_.isString(definition)) {
		return isNamedType(definition);
	} else {
		return isNamedType(definition?.type);
	}
}

const resolveSchemaUdt = schema => {
	const type = _.isString(schema) || _.isArray(schema) ? schema : schema.type;
	if (isNativeType(type)) {
		return schema;
	}

	if (_.isString(schema)) {
		return getTypeFromUdt(schema);
	}

	if (_.isArray(type)) {
		return { ...(_.isArray(schema) ? {} : schema), type: type.map(resolveSchemaUdt) };
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

	const { schema } = getUdtItem(type) || {};
	let udtItem = resolveSymbolDefaultValue(schema);

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

	if (!udtItem.schema.namespace) {
		return type;
	}

	return udtItem.schema.namespace + '.' + type;
};

const convertNamedTypesToReferences = schema => {
	if (!isNamedType(schema.type)) {
		return schema;
	}

	if (!udt[schema.name] || !isUdtUsed(schema.name)) {
		udt[schema.name] = { schema };
	}

	return simplifySchema(convertSchemaToReference(schema));
};

const convertSchemaToReference = schema => {
	_ = dependencies.lodash;

	const referenceAttributes = filterAttributes(_.omit(schema, 'type'));

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

const convertCollectionReferences = entities => {
	_ = dependencies.lodash;

	const entitiesIds = entities.map(entity => entity.jsonSchema.GUID);
	const entitiesWithReferences = entities.map(entity => {
		let references = [];
		const mapper = mapJsonSchema((field, path) => {
			if (!field.ref) {
				return field;
			}

			const isCollectionRef = !!field.parentCollectionName;
			if (!entitiesIds.includes(field.ref)) {
				if (!isCollectionRef) {
					return field;
				}
				references = [...references, { name: field.parentCollectionName }];

				return {
					...field,
					$ref: `#/definitions/${field.parentCollectionName}`,
					namespace: field.namespace || field.parentBucketName,
					default: field.nullable ? null : field.default,
				};
			}

			const definition = entities.find(entity => entity.jsonSchema.GUID === field.ref).jsonSchema;
			const definitionName = definition.code || definition.collectionName || definition.name;

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
				path,
				version: getConfluentSchemaVersion(definition.confluentVersion),
			}];

			return {
				...field,
				$ref: `#/definitions/${definitionName}`,
				default: field.nullable ? null : field.default,
			};
		});

		const jsonSchema = mapper(entity.jsonSchema);

		addDefinitions(references.reduce((definitions, reference) => ({
			...definitions,
			[reference.name]: {
				isCollectionReference: true,
				schema: {},
			}
		}), {}));

		return {
			...entity,
			jsonSchema,
			references: filterReferencesByPath(entity, references).map(reference => _.omit(reference, 'path')),
		};
	});

	return topologicalSort(entitiesWithReferences);
};

const filterReferencesByPath = (entity, references) => references.filter(currentReference => {
	const isRecursive = _.last(currentReference.path) === entity?.jsonSchema?.GUID;
	if (isRecursive) {
		return false;
	}

	const rootReference = references.find(reference => {
		if (!reference.path || (reference.path.length >= currentReference.path?.length)) {
			return false;
		}

		return reference.path.every((path, index) => path === currentReference.path?.[index]);
	});

	return !rootReference;
});

const resolveNamespaceReferences = entities => {
	_ = dependencies.lodash;

	const entitiesWithReferences = entities.map(entity => {
		const mapper = mapJsonSchema(field => {
			if (!field.ref) {
				return field;
			}

			const isCollectionRef = !!field.parentCollectionName;
			if (!isCollectionRef) {
				return field;
			}

			return _.omit(field, '$ref');
		});

		const jsonSchema = mapper(entity.jsonSchema);

		return {
			...entity,
			jsonSchema,
		};
	});

	return entitiesWithReferences;
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
   convertCollectionReferences,
   resolveNamespaceReferences,
};
