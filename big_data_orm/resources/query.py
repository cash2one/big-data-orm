import logging
import re

from big_data_orm.resources.column import Column
from big_data_orm.resources.utils.mock_data_generator import MockDataGenerator

BEGIN_DATE = '2010-01-01'
END_DATE = '2030-01-01'

NUMBER_OF_MOCK_SAMPLES = 10


class Query(object):
    def __init__(self, columns, table_name, dataset_id, is_partitioned):
        self.is_partitioned = is_partitioned
        self.begin_date = BEGIN_DATE
        self.end_date = END_DATE
        self.table_name = dataset_id + '.' + table_name
        self.query_data = {}
        self.columns = columns
        self.query_data['columns'] = self.columns
        self._check_column_types()
        self.flatten_active = False

    def _check_column_types(self):
        for column in self.columns:
            if column.column_type is dict:
                logging.error("Trying to use DICT " +
                              "columns at query. Please use the leaf column instead")

    def filter(self, clause):
        """
        Filter method for ORM.
        Arg clause already arrive here as a dict with op information.
        """
        if not clause:
            return self
        if 'filters' not in self.query_data.keys():
            self.query_data['filters'] = []
        self.query_data['filters'].append(clause)
        return self

    def filter_by_date(self, begin_date, end_date):
        """
        Define the begin and end date for the partitions that will be included.
        """
        if not self._check_date_args(begin_date) or not self._check_date_args(end_date):
            logging.warning("Invalid filter_by_date arguments. Must be \'YEAR-MM-DD\'")
            logging.warning("Using 2010-01-01 and 2030-01-01 as date filters.")
            return self
        self.begin_date = begin_date
        self.end_date = end_date
        return self

    def _check_date_args(self, argument):
        pattern = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
        return pattern.match(argument)

    def order_by(self, column, desc=False):
        if not self._column_is_present(column.name):
            logging.error("Trying to order by a non existing column.")
            return self
        order = {
            'column': column.name,
            'desc': desc
        }
        if 'orders' not in self.query_data.keys():
            self.query_data['orders'] = []
        self.query_data['orders'].append(order)
        return self

    def flatten(self, field):
        self.table_name = "FLATTEN({}, {})".format(self.table_name, field.name)
        self.flatten_active = True
        return self

    def all(self, session, debug=False):
        """
        Return all the results from BigQuery
        """
        if debug:
            mock_generator = MockDataGenerator()
            return mock_generator.generate_data(NUMBER_OF_MOCK_SAMPLES, self.columns)

        query = self.assemble()
        return session.run_query(query)

    def first(self, session, debug=False):
        """
        Return the first element as a dict
        """
        if debug:
            mock_generator = MockDataGenerator()
            return mock_generator.generate_data(NUMBER_OF_MOCK_SAMPLES, self.columns)

        query = self.assemble()
        response = session.run_query(query)
        try:
            return response[0]
        except KeyError:
            logging.error("Query result is not a list. Returning all results...")
            return response

    def limit(self, value):
        limit = {'value': value}
        self.query_data['limit'] = limit
        return self

    def assemble(self):
        """
        Build the query as a string.
        """
        if self.is_partitioned:
            sql_query = str('SELECT {} FROM {} ' +
                            'WHERE _PARTITIONTIME BETWEEN TIMESTAMP(\'{}\') AND TIMESTAMP(\'{}\')')
        else:
            sql_query = str('SELECT {} FROM {}')

        fields = ''.join(str(column.name) + ', ' for column in self.columns)
        fields = fields[:-2]

        if 'filters' in self.query_data.keys():
            sql_query += self._build_filters_clause()

        if 'orders' in self.query_data.keys():
            sql_query += self._build_orders_clause()

        if 'limit' in self.query_data.keys():
            sql_query += self._build_limit_clause()

        if self.is_partitioned:
            return sql_query.format(fields, self.table_name, self.begin_date, self.end_date)
        else:
            return sql_query.format(fields, self.table_name)

    def _build_limit_clause(self):
        return ' LIMIT {}'.format(self.query_data['limit']['value'])

    def _build_orders_clause(self):
        query = ''
        not_first_clause = False
        for order in self.query_data['orders']:
            order_or_comma = 'ORDER BY' if not not_first_clause else ','
            query += ' {} {} DESC'.format(order_or_comma, order['column']) if order['desc'] \
                else ' {} {}'.format(order_or_comma, order['column'])
            not_first_clause = True
        return query

    def _build_filters_clause(self):
        """
        Build all the SQL that comes after WHERE statement.
        Return:
            (str) Full WHERE clause.
            Example:
                "WHERE foo == 10 and bar < 1 OR bar > -1"
        """
        query = ' AND '
        not_first_clause = False
        for filter_clause in self.query_data['filters']:
            if filter_clause['type'] is 'and':
                query += self._build_and_operation(filter_clause, not_first_clause)
                not_first_clause = True
            elif filter_clause['type'] is 'or':
                query += self._build_or_operation(filter_clause, not_first_clause)
                not_first_clause = True
        return query

    def _build_and_operation(self, filter_clause, not_first_clause):
        """
        Build the where clause as an AND operation.
        Args:
            filter_clause: (dict) Clause elements.
            not_first_clause: (bool) True if this
            is not the first clause of the query.
        Return:
            (str) clause as SQL.
            Example: " field > 100".
            Or even " and test = 'raccoon'".
        """
        partial_query = ' AND ' if not_first_clause else ''
        partial_query += self._build_clause_core(filter_clause)
        return partial_query

    def _build_clause_core(self, filter_clause):
        """
        Build the core of the clause.
        Args:
            filter_clause: (dict) Clause elements.
        Return:
            (str) Example: "field != 'foo-bar'"
        """
        clause_sql = '{} {} {}'
        right_value = filter_clause['right_value']
        if filter_clause['right_value_type'] is str:
            clause_sql = '{} {} \'{}\''
        if filter_clause['right_value_type'] is list:
            right_value = self._parse_in_list(filter_clause['right_value'])
        return clause_sql.format(filter_clause['left_value'], filter_clause['signal'], right_value)

    def _build_or_operation(self, filter_clause, not_first_clause):
        """
        An OR operation is represented by a dict with the both sides of the operation.
        Args:
            filter_clause: (dict) All the operation data.
        Return:
            (str) String with the OR clause already built.
            Example: field = 'test' OR other_field = 'other_test'
        """
        partial_query = ' and ' if not_first_clause else ''
        clause_sql = '{} OR {}'
        clause_sql = clause_sql.format(
            self._build_clause_core(filter_clause['left_side']),
            self._build_clause_core(filter_clause['right_side'])
        )
        return partial_query + clause_sql

    def _parse_in_list(self, values):
        in_clause_query = "("
        for v in values:
            if type(v) is str:
                in_clause_query += '\'' + str(v) + '\'' + ", "
            else:
                in_clause_query += str(v) + ", "
        in_clause_query = in_clause_query[:-2] + ")"
        return in_clause_query

    def _check_filter_columns(self, op):
        """
        Check if the columns present at operation are present at the
        query columns.
        """
        if not self._column_is_present(op['left_value']):
            logging.error("Column (left_value) not present at query columns.")
            return False
        if op['right_value_type'] is Column:
            if not self._column_is_present(op['right_value']):
                logging.error("Column (right_value) not present at query columns.")
                return False
            return True
        return True

    def _column_is_present(self, column_name):
        for column in self.columns:
            if column.name == column_name:
                return True
        return False
