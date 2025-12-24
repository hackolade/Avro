const _ = require('lodash');
const { filterMultipleTypes, prepareName, getDefaultName, convertName } = require('./generalHelper');
const getTypeFromReference = require('./getTypeFromReference');
const { AVRO_TYPES } = require('../../shared/constants');
const { getFieldCustomProperties } = require('../../shared/customProperties');

const CHOICES = ['oneOf', 'anyOf', 'allOf'];

const convertChoicesToProperties = schema => {
	return CHOICES.reduce((schema, choice) => convertChoiceToProperties(schema, choice), schema);
};

const subSchemaIsEntityReference = subSchema => subSchema.$ref;
const convertChoiceToProperties = (schema, choice) => {
	if (!schema[choice]) {
		return schema;
	}
	const choiceMeta = schema[getChoiceMetaKeyword(choice)] || {};
	if (_.isArray(choiceMeta)) {
		return handleMergedChoice(schema, choiceMeta);
	}

	const allSubSchemaFields = schema[choice]
		.map(convertChoicesToProperties)
		.flatMap(subSchema => {
			if (subSchemaIsEntityReference(subSchema)) {
				return [
					{
						...subSchema,
						type: getTypeFromReference(subSchema),
					},
				];
			}

			if (subSchema.type === 'array') {
				return subSchema.items;
			}

			return Object.keys(subSchema.properties || {}).map(key => ({
				name: prepareName(key),
				...subSchema.properties[key],
			}));
		})
		.filter(item => !_.isEmpty(item));

	if (schema.type === 'array') {
		return {
			...schema,
			items: [...(schema.items || []).filter(item => !_.isEmpty(item)), ...allSubSchemaFields],
		};
	}

	// custom properties of choice have higher priority than custom properties of fields in subschemas
	const choiceCustomProperties = getFieldCustomProperties({ schema: { ...choiceMeta, type: 'choice' } });

	const choiceName =
		choiceMeta.code ||
		choiceMeta.name ||
		allSubSchemaFields[0]?.code ||
		allSubSchemaFields[0]?.name ||
		getDefaultName();

	const fieldWithDescription = allSubSchemaFields.findLast(field => field.description || field.refDescription);
	const choiceDescription =
		choiceMeta.description || fieldWithDescription?.description || fieldWithDescription?.refDescription;

	const multipleFieldsHash = allSubSchemaFields.reduce((multipleFieldsHash, field, index) => {
		const multipleField = multipleFieldsHash[choiceName] || {
			...choiceMeta,
			default: convertDefaultMetaFieldType(field.type, choiceMeta.default),
			name: prepareName(choiceName),
			type: [],
			choiceMeta,
		};
		const multipleTypeAttributes = {
			...field,
			// When the choice have the description property, we show it and ignore the property in the fields
			// when the field is not a reference. If it is a reference, the field can have the description of the
			// but it will come from the definition and its not handled here.
			//
			// When the choice have no description we take the description of a field and put it into choice, at the same
			// time removing it from field.
			description: undefined,
			type: field.$ref ? getTypeFromReference(field) : field.type,
			name: prepareName(field.code || field.name || choiceName),
		};
		const multipleTypes = ensureArray(multipleField.type).concat(multipleTypeAttributes);
		const type = _.isArray(multipleTypes)
			? multipleTypes.map(typeSchema =>
					['fixed', 'enum', 'record'].includes(typeSchema?.type)
						? typeSchema
						: typeSchema?.type || typeSchema,
				)
			: multipleTypes?.type || multipleTypes;
		const defaultFromSubschema = index === 0 ? multipleTypeAttributes.default : undefined;
		const defaultValue = !_.isUndefined(multipleField.default) ? multipleField.default : defaultFromSubschema;

		return {
			...multipleFieldsHash,
			[choiceName]: {
				...convertName(multipleField),
				...convertName(multipleTypeAttributes),
				...choiceCustomProperties,
				...(choiceDescription && { description: choiceDescription }),
				default: defaultValue,
				type,
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
			return [...choices, { items, type: 'allOf', meta }];
		}

		const choiceItems = _.first(items)[type];

		return [...choices, { items: choiceItems, type, meta }];
	}, []);

	const newSchema = separateChoices.reduce((updatedSchema, choiceData) => {
		const choiceType = choiceData.type;
		const schemaWithChoice = {
			...removeChoices(updatedSchema),
			[choiceType]: choiceData.items,
			[getChoiceMetaKeyword(choiceType)]: choiceData.meta,
		};

		return convertChoiceToProperties(schemaWithChoice, choiceType);
	}, schema);

	return { ...schema, ...newSchema };
};

const getChoiceMetaKeyword = choiceKeyword => `${choiceKeyword}_meta`;

const removeChoices = schema =>
	_.omit(
		schema,
		CHOICES.flatMap(choice => [choice, getChoiceMetaKeyword(choice)]),
	);

const convertDefaultMetaFieldType = (type, value) => {
	if (type === 'null' && value === 'null') {
		return null;
	}

	if (type === 'number' && !isNaN(value)) {
		return Number(value);
	}

	return value;
};

const ensureArray = (item = []) => (_.isArray(item) ? item : [item]);

const getChoiceIndex = choice => _.get(choice, 'choiceMeta.index');

const addPropertiesFromChoices = (properties, choiceProperties) => {
	if (_.isEmpty(choiceProperties)) {
		return properties || {};
	}

	const typesUsedInChoicesProperties = Object.values(choiceProperties)
		.flatMap(({ type }) => type)
		.filter(type => !AVRO_TYPES.includes(type));
	const propertiesEntries = Object.entries(properties || {})
		.filter(([key, _]) => !typesUsedInChoicesProperties.includes(key))
		.map(([key, property], index) => {
			return [key, { ...property, choiceMeta: { index } }];
		});

	return Object.fromEntries(
		[...Object.entries(choiceProperties), ...propertiesEntries].sort(
			([key1, choice1], [key2, choice2]) => getChoiceIndex(choice1) - getChoiceIndex(choice2),
		),
	);
};

module.exports = convertChoicesToProperties;
