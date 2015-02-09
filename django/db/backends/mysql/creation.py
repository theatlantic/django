import subprocess
import sys

from django.db.backends.base.creation import BaseDatabaseCreation

from .client import DatabaseClient


class DatabaseCreation(BaseDatabaseCreation):

    def sql_table_creation_suffix(self):
        suffix = []
        test_settings = self.connection.settings_dict['TEST']
        if test_settings['CHARSET']:
            suffix.append('CHARACTER SET %s' % test_settings['CHARSET'])
        if test_settings['COLLATION']:
            suffix.append('COLLATE %s' % test_settings['COLLATION'])
        return ' '.join(suffix)

    def sql_for_inline_foreign_key_references(self, model, field, known_models, style):
        "All inline references are pending under MySQL"
        return [], True

    def sql_destroy_indexes_for_fields(self, model, fields, style):
        if len(fields) == 1 and fields[0].db_tablespace:
            tablespace_sql = self.connection.ops.tablespace_sql(fields[0].db_tablespace)
        elif model._meta.db_tablespace:
            tablespace_sql = self.connection.ops.tablespace_sql(model._meta.db_tablespace)
        else:
            tablespace_sql = ""
        if tablespace_sql:
            tablespace_sql = " " + tablespace_sql

        field_names = []
        qn = self.connection.ops.quote_name
        for f in fields:
            field_names.append(style.SQL_FIELD(qn(f.column)))

        index_name = "%s_%s" % (model._meta.db_table, self._digest([f.name for f in fields]))

        from ..utils import truncate_name

        return [
            style.SQL_KEYWORD("DROP INDEX") + " " +
            style.SQL_TABLE(qn(truncate_name(index_name, self.connection.ops.max_name_length()))) + " " +
            style.SQL_KEYWORD("ON") + " " +
            style.SQL_TABLE(qn(model._meta.db_table)) + ";",
        ]

    def _clone_test_db(self, number, verbosity, keepdb=False):
        qn = self.connection.ops.quote_name
        source_database_name = self.connection.settings_dict['NAME']
        target_database_name = self.get_test_db_clone_settings(number)['NAME']

        with self._nodb_connection.cursor() as cursor:
            try:
                cursor.execute("CREATE DATABASE %s" % qn(target_database_name))
            except Exception as e:
                if keepdb:
                    return
                try:
                    if verbosity >= 1:
                        print("Destroying old test database '%s'..." % self.connection.alias)
                    cursor.execute("DROP DATABASE %s" % qn(target_database_name))
                    cursor.execute("CREATE DATABASE %s" % qn(target_database_name))
                except Exception as e:
                    sys.stderr.write("Got an error recreating the test database: %s\n" % e)
                    sys.exit(2)

        dump_cmd = DatabaseClient.settings_to_cmd_args(self.connection.settings_dict)
        dump_cmd[0] = 'mysqldump'
        dump_cmd[-1] = source_database_name
        load_cmd = DatabaseClient.settings_to_cmd_args(self.connection.settings_dict)
        load_cmd[-1] = target_database_name

        dump_proc = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE)
        load_proc = subprocess.Popen(load_cmd, stdin=dump_proc.stdout, stdout=subprocess.PIPE)
        dump_proc.stdout.close()    # allow dump_proc to receive a SIGPIPE if load_proc exits.
        load_proc.communicate()
