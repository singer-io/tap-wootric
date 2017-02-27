import datetime


class InvalidData(Exception):
    """Raise when data doesn't validate the schema"""


def ms_to_datetime(value):
    return datetime.datetime.utcfromtimestamp(int(value)).strptime("%Y-%m-%dT%H:%M:%SZ")


def us_to_datetime(value):
    return ms_to_datetime(int(value) * 0.001)


class Transformer(object):
    def __init__(self, schema, datetime_validator=None):
        self.schema = schema
        if datetime_validator:
            self.transform_datetime = datetime_validator
        else:
            self.transform_datetime = lambda x: x

    def transform(self, row):
        return self._transform_field(row, self.schema)

    def _anyOf(self, data, schema_list):
        for schema in schema_list:
            try:
                return self.transform_field(data, schema)
            except:
                pass

        raise InvalidData("{} doesn't match any of {}".format(data, schema_list))

    def _array(self, data, items_schema):
        return [self._transform_field(value, items_schema) for value in data]

    def _object(self, data, properties_schema):
        return {field: self._transform_field(data[field], field_schema)
                for field, field_schema in properties_schema.items()
                if field in data}

    def _type_transform(self, value, type_schema):
        if isinstance(type_schema, list):
            for typ in type_schema:
                try:
                    return self._type_transform(value, typ)
                except:
                    pass

            raise InvalidData("{} doesn't match any of {}".format(value, type_schema))

        if not value:
            if type_schema != "null":
                raise InvalidData("Null is not allowed")
            else:
                return None

        if type_schema == "string":
            return str(value)

        if type_schema == "integer":
            return int(value)

        if type_schema == "number":
            return float(value)

        if type_schema == "boolean":
            return bool(value)

        raise InvalidData("Unknown type {}".format(type_schema))

    def _format_transform(self, value, format_schema):
        if format_schema == "date-time":
            return self._transform_datetime(value)

        raise InvalidData("Unknown format {}".format(format_schema))

    def _transform_field(self, value, field_schema):
        if "anyOf" in field_schema:
            return self._anyOf(value, field_schema["anyOf"])

        if field_schema["type"] == "array":
            return self._array(value, field_schema["items"])

        if field_schema["type"] == "object":
            return self._object(value, field_schema["properties"])

        value = self._type_transform(value, field_schema["type"])
        if "format" in field_schema:
            value = self._format_transform(value, field_schema["format"])

        return value
