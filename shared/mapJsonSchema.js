const { dependencies } = require('./appDependencies');

let _;

const PROPERTIES_LIKE = ['properties', 'definitions', 'patternProperties'];
const ITEMS_LIKE = ['items', 'oneOf', 'allOf', 'anyOf', 'not'];

const mapJsonSchema =
	(callback, path = []) =>
	jsonSchema => {
		_ = dependencies.lodash;
		if (!_.isPlainObject(jsonSchema)) {
			return jsonSchema;
		}

		const currentPath = [...path, jsonSchema.GUID];
		const mapper = mapJsonSchema(callback, currentPath);
		const jsonSchemaWithNewProperties = applyTo(PROPERTIES_LIKE, { ...jsonSchema }, mapProperties(mapper));
		const newJsonSchema = applyTo(ITEMS_LIKE, jsonSchemaWithNewProperties, mapItems(mapper));

		return callback(newJsonSchema, currentPath);
	};

const mapProperties = mapper => properties =>
	Object.keys(properties).reduce(
		(newProperties, key) => ({
			...newProperties,
			[key]: mapper(properties[key]),
		}),
		{},
	);

const mapItems = mapper => items => {
	if (_.isArray(items)) {
		return items.map(mapper);
	} else if (_.isPlainObject(items)) {
		return mapper(items);
	}

	return items;
};

const applyTo = (properties, jsonSchema, mapper) =>
	properties.reduce((jsonSchema, key) => {
		if (!jsonSchema[key]) {
			return jsonSchema;
		}

		return { ...jsonSchema, [key]: mapper(jsonSchema[key]) };
	}, jsonSchema);

module.exports = mapJsonSchema;
