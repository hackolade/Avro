const { dependencies } = require('../appDependencies');
const mapJsonSchema = require('./mapJsonSchema');
const { getNamespace, getName, EMPTY_NAMESPACE } = require('./generalHelper');

let _;
let definitions = { [EMPTY_NAMESPACE]: [] };

const resolveRootReference = schema => {
	if (!schema.$ref) {
		return schema;
	}

	return findDefinition(getNamespace(schema) || EMPTY_NAMESPACE, getName(schema));
};

const getDefinitions = () => Object.keys(definitions).reduce((jsonDefinitions, namespace) => ({
	...jsonDefinitions,
	...definitions[namespace],
}), {});

const addDefinition = (namespace, definition) => {
	const name = definition.name;
	dependencies.lodash.set(definitions, [namespace, name], definition);

	return { definitionName: name, $ref: name, name, namespace };
};

const filterUnusedDefinitions = schema => ({
	...schema,
	definitions: dependencies.lodash.pick(schema.definitions, getUsedDefinitionsWithRelated(schema)),
});

const getUsedDefinitionsWithRelated = schema => {
	let usedDefinitions = [];
	mapJsonSchema(field => {
		if (!field.$ref) {
			return field;
		}
		const definition = findDefinition(field.namespace, field.definitionName);
		if (!definition) {
			return field;
		}

		const relatedDefinitions = getUsedDefinitions(definition);
		usedDefinitions = [ ...usedDefinitions, field.definitionName, ...relatedDefinitions ];

		return field;
	})(dependencies.lodash.omit(schema, 'definitions'));

	return sortDefinitionsNames(schema, usedDefinitions);
};

const getUsedDefinitions = schema => {
	let usedDefinitions = [];
	mapJsonSchema(field => {
		if (!field.$ref) {
			return field;
		}
		const definition = findDefinition(field.namespace, field.definitionName);
		if (!definition) {
			return field;
		}

		usedDefinitions = [ ...usedDefinitions, field.definitionName ];

		return field;
	})(schema);

	return usedDefinitions;
};

const sortDefinitionsNames = (schema, definitionsNames) => Object.keys(schema.definitions).filter(name => definitionsNames.includes(name));

const updateRefs = mapJsonSchema(field => field.$ref ? updateRef(field) : field);

const updateRef = ({ name, namespace, definitionName, $ref }) => {
	if (findDefinition(namespace, definitionName)) {
		return { name, $ref: `#/definitions/${definitionName}`};
	}

	return { name, $ref, hackoladeMeta: { restrictExternalReferenceCreation: true } };
};

const findDefinition = (namespace, name) => definitions[namespace]?.[name];

module.exports = {
    addDefinition,
    resolveRootReference,
    getDefinitions,
    filterUnusedDefinitions,
    updateRefs
};
