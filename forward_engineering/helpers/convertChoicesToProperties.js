const { dependencies } = require('../../shared/appDependencies');
const { filterMultipleTypes, prepareName } = require('./generalHelper');
let _;

const CHOICES = ['oneOf', 'anyOf', 'allOf'];

const convertChoicesToProperties = schema => {
    _ = dependencies.lodash;

    return CHOICES.reduce((schema, choice) => convertChoiceToProperties(schema, choice), schema);
};

const convertChoiceToProperties = (schema, choice) => {
	if (!schema[choice]) {
		return schema;
	}
	const choiceMeta = schema[getChoiceMetaKeyword(choice)] || {};
	if (_.isArray(choiceMeta)) {
		return handleMergedChoice(schema, choiceMeta);
	}

	const allSubSchemaFields = schema[choice].map(convertChoicesToProperties).flatMap(subSchema => {
		if (subSchema.type === 'array') {
			return subSchema.items;
		}

		return Object.keys(subSchema.properties || {}).map(key => ({ name: prepareName(key), ...subSchema.properties[key]}));
	}).filter(item => !_.isEmpty(item))

	if (schema.type === 'array') {
		return {
			...schema,
			items: [ ...(schema.items || []).filter(item => !_.isEmpty(item)), ...allSubSchemaFields ]
		};
	}

	const multipleFieldsHash = allSubSchemaFields.reduce((multipleFieldsHash, field) => {
		const fieldName = choiceMeta.code || choiceMeta.name || field.name;
		const multipleField = multipleFieldsHash[fieldName] ||
			{
				...choiceMeta,
				default: convertDefaultMetaFieldType(field.type, choiceMeta.default),
				name: prepareName(fieldName),
				type: [],
				choiceMeta
			};
		const multipleTypes = ensureArray(multipleField.type).concat({ ...field, name: prepareName(field.name || fieldName) });

		return {
			...multipleFieldsHash,
			[fieldName]: {
				...multipleField,
				type: filterMultipleTypes(multipleTypes),
			},
		};
	}, {});

	return {
		...schema,
		properties: addPropertiesFromChoices(schema.properties, multipleFieldsHash),
	};
};
const handleMergedChoice = (schema, choiceMeta) => {
	const separateChoices = choiceMeta.reduce((choices, meta) => {
		const items = schema.allOf.filter(item => (meta?.ids || []).includes(item.GUID));
		const type = meta?.choice;
		if (!type || type === 'allOf') {
			return [ ...choices, { items, type: 'allOf', meta } ];
		}

		const choiceItems = _.first(items)[type];

		return [ ...choices, { items: choiceItems, type, meta } ];
	}, []);
	
	const newSchema = separateChoices.reduce((updatedSchema, choiceData) => {
		const choiceType = choiceData.type;
		const schemaWithChoice = {
			...removeChoices(updatedSchema),
			[choiceType]: choiceData.items,
			[getChoiceMetaKeyword(choiceType)]: choiceData.meta
		};

		return convertChoiceToProperties(schemaWithChoice, choiceType);
	}, schema);

	return { ...schema, ...newSchema };
};

const getChoiceMetaKeyword = choiceKeyword => `${choiceKeyword}_meta`;

const removeChoices = schema => _.omit(schema, CHOICES.flatMap(choice => [choice, getChoiceMetaKeyword(choice)]));

const convertDefaultMetaFieldType = (type, value) => {
	if (type === 'null' && value === 'null') {
		return null;
	}

	if (type === 'number' && !isNaN(value)) {
		return Number(value);
	}

	return value;
};

const ensureArray = (item = []) => _.isArray(item) ? item : [item];

const getChoiceIndex = choice => _.get(choice, 'choiceMeta.index');

const addPropertiesFromChoices = (properties, choiceProperties) => {
	if (_.isEmpty(choiceProperties)) {
		return properties || {};
	}

	const propertiesEntries = Object.entries(properties || {}).map(([key, property], index) => {
		return [ key, { ...property, choiceMeta: { index }} ];
	});

	return Object.fromEntries(
		[
			...Object.entries(choiceProperties),
			...propertiesEntries
		].sort(([ key1, choice1], [key2, choice2]) => getChoiceIndex(choice1) - getChoiceIndex(choice2))
	);
};

module.exports = convertChoicesToProperties;
