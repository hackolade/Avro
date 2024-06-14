const { dependencies } = require('../../shared/appDependencies');
const mapJsonSchema = require('../../shared/mapJsonSchema');
const { getNamespace, getName, EMPTY_NAMESPACE } = require('./generalHelper');

let definitions = { [EMPTY_NAMESPACE]: [] };

const resolveRootReference = schema => {
	if (!schema.$ref) {
		return schema;
	}

	return findDefinition(getNamespace(schema) || EMPTY_NAMESPACE, getName(schema));
};

const getDefinitions = () =>
	Object.keys(definitions).reduce(
		(jsonDefinitions, namespace) => ({
			...jsonDefinitions,
			...definitions[namespace],
		}),
		{},
	);

const addDefinition = (namespace, definition) => {
	const name = definition.name;
	dependencies.lodash.set(definitions, [namespace, name], definition);

	return {
		definitionName: name,
		$ref: name,
		name,
		namespace,
		...(!dependencies.lodash.isUndefined(definition.default) && { default: definition.default }),
	};
};

const filterUnusedDefinitions = schema => ({
	...schema,
	definitions: dependencies.lodash.pick(schema.definitions, sortDefinitionsNames(schema, getUsedDefinitions(schema))),
});

const getUsedDefinitions = (schema, parentDefinitions = []) => {
	let usedDefinitions = [];
	mapJsonSchema(field => {
		if (!field.$ref) {
			return field;
		}
		if (parentDefinitions.includes(field.definitionName)) {
			return field;
		}
		const definition = findDefinition(field.namespace, field.definitionName);
		if (!definition) {
			return field;
		}

		const relatedDefinitions = getUsedDefinitions(definition, [
			...parentDefinitions,
			...usedDefinitions,
			field.definitionName,
		]);
		usedDefinitions = [...usedDefinitions, field.definitionName, ...relatedDefinitions];

		return field;
	})(schema);

	return usedDefinitions;
};

const sortDefinitionsNames = (schema, definitionsNames) =>
	Object.keys(schema.definitions).filter(name => definitionsNames.includes(name));

const updateRefs = mapJsonSchema(field => (field.$ref ? updateRef(field) : field));

const updateRef = ({ name, namespace, description, definitionName, $ref, nullable, subschema }) => {
	if (findDefinition(namespace, definitionName)) {
		return { name, description, $ref: `#/definitions/${definitionName}` };
	}

	if ($ref.startsWith('#collection/')) {
		return { name, description, $ref, nullable, subschema };
	}

	return { name, description, $ref, hackoladeMeta: { restrictExternalReferenceCreation: true }, type: 'reference' };
};

const findDefinition = (namespace, name) => definitions[namespace]?.[name];
const isBareUnionSchema = (schema, type) =>
	!schema.fields && !type && Object.keys(schema).some(key => !isNaN(parseInt(key)));

module.exports = {
	addDefinition,
	resolveRootReference,
	getDefinitions,
	filterUnusedDefinitions,
	updateRefs,
	isBareUnionSchema,
};
