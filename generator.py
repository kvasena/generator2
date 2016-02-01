import sys
import yaml


class WrongYamlFileException(Exception):
    pass


class Generator(object):
    result = set()

    __create__ = '\nCREATE TABLE \"{table}\" (' \
                 '\n\t\"{table}_id\" SERIAL PRIMARY KEY NOT NULL, {fields}'\
                 '\n\t\"{table}_created\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,'\
                 '\n\t\"{table}_updated\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);'
    __fields__ = '\n\t\"{table}_{column}\" {value} NOT NULL,'
    __foreign_key__ = '\nALTER TABLE \"{table}\" ADD FOREIGN KEY (\"{parent}_id\") REFERENCES' \
                      '\"{parent}\" (\"{parent}_id\") ON UPDATE CASCADE ON DELETE CASCADE;'
    __create_relation_table__ = '\nCREATE TABLE \"{table1}_{table2}\" (' \
                                '\"{table1}_{table2}_id\" SERIAL PRIMARY KEY NOT NULL, ' \
                                '\n\t\"{table1}_id\" FOREIGN KEY (\"{table1}_id\") REFERENCES ' \
                                '\"{table1}\" (\"{table1}_id\") ON UPDATE CASCADE ON DELETE CASCADE,'\
                                '\n\t\"{table2}_id\" FOREIGN KEY (\"{table2}_id\") REFERENCES ' \
                                '\"{table2}\" (\"{table2}_id\") ON UPDATE CASCADE ON DELETE CASCADE);'
    __trigger__ = "\nCREATE OR REPLACE FUNCTION update() RETURNS TRIGGER AS " \
                  "\n\t'BEGIN NEW.updated = NOW();\n\tRETURN NEW; \n\tEND;\n\t'LANGUAGE 'plpgsql';" \
                  "\nCREATE TRIGGER update_trigger BEFORE UPDATE ON \"{table}\"" \
                  "FOR EACH ROW EXECUTE PROCEDURE update();\n"

    def __init__(self, data):
        self.data = data

    def __create_table_statement(self):
        for key, value in self.data.iteritems():
            table_name = key.lower()
            field_dict = value['fields']
            field_values = []
            for key, value in field_dict.iteritems():
                 field_values.append(self.__fields__.format(table=table_name, column=key, value=value))

            self.result.add(self.__create__.format(table=table_name, fields=''.join(field_values)))

    def __add_trigger_statement(self):
        for key in self.data.iterkeys():
            self.result.add(self.__trigger__.format(table=key.lower()))

    def __relations__(self):
        for key, value in self.data.iteritems():
            table_name = key
            relation_dict = value['relations']
            for key, value in relation_dict.iteritems():
                if value == 'one' and self.data[key]['relations'].has_key(table_name):
                        self.result.add(self.__foreign_key__.format(table=table_name.lower(), parent=key.lower()))
                elif value == 'many' and self.data[key]['relations'].has_key(table_name):
                        if self.data[key]['relations'][table_name] == 'many':
                            self.__relation_table__(key, table_name)
                        else:
                            pass
                else:
                    raise WrongYamlFileException()

    def __relation_table__(self, table1, table2):
        tables = [table1.lower(), table2.lower()]
        tables.sort()
        self.result.add(self.__create_relation_table__.format(table1=tables[0], table2=tables[1]))

    def dump(self):
        self.__create_table_statement()
        self.__add_trigger_statement()
        self.__relations__()
        return ''.join(self.result)


def main():
    file_in = open("schema.txt", 'r')
    text = yaml.load(file_in)
    file_in.close()
    generator = Generator(text)
    file_out = open("result.sql", 'w')
    file_out.write(generator.dump())
    file_out.close()


if __name__ == "__main__":
    main()

