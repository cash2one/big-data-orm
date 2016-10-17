import logging

from big_data_orm.resources.column import Column


class Query(object):
    def __init__(self, columns, table_name):
        self.table_name = table_name
        self.query_data = {}
        self.columns = columns
        self.query_data['columns'] = self.columns

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

    def all(self, session, newest_only=False, filter_key=''):
        query = self.assemble()
        return session.run_query(query, newest_only=newest_only, filter_key=filter_key)

    def first(self, session, newest_only=False, filter_key=''):
        """
        Return the first element as a dict
        """
        query = self.assemble()
        return session.run_query(query, newest_only=newest_only, filter_key=filter_key)[0]

    def limit(self, value):
        limit = {
            'value': value
        }
        self.query_data['limit'] = limit
        return self

    def assemble(self):
        sql_query = 'SELECT {} FROM {}'
        fields = ''
        for column in self.columns:
            fields += str(column.name) + ', '
        fields = fields[:-2]

        if 'filters' in self.query_data.keys():
            sql_query += self._build_filters_clause()

        if 'orders' in self.query_data.keys():
            sql_query += self._build_orders_clause()

        if 'limit' in self.query_data.keys():
            sql_query += self._build_limit_clause()

        sql_query = sql_query.format(fields, self.table_name, '')
        return sql_query

    def _build_limit_clause(self):
        query = ' LIMIT {}'.format(self.query_data['limit']['value'])
        return query

    def _build_orders_clause(self):
        query = ''
        not_first_clause = False
        for order in self.query_data['orders']:

            if not not_first_clause:
                order_or_comma = 'ORDER BY'
            else:
                order_or_comma = ','

            if order['desc']:
                query += ' {} {} DESC'.format(order_or_comma, order['column'])
            else:
                query += ' {} {}'.format(order_or_comma, order['column'])

            not_first_clause = True
        return query

    def _build_filters_clause(self):
        query = ' WHERE '
        not_first_clause = False
        for filter_clause in self.query_data['filters']:
            clause_sql = '{} {} {}'
            right_value = filter_clause['right_value']

            if not_first_clause:
                query += ' and '

            if filter_clause['right_value_type'] is str:
                clause_sql = '{} {} \'{}\''

            if filter_clause['right_value_type'] is list:
                right_value = self._parse_in_list(filter_clause['right_value'])

            query += clause_sql.format(
                filter_clause['left_value'], filter_clause['signal'],
                right_value
            )
            not_first_clause = True
        return query

    def _parse_in_list(self, values):
        in_clause_query = "("
        for v in values:
            if type(v) is str:
                in_clause_query += '\'' + str(v) + '\'' + ","
            else:
                in_clause_query += str(v) + ","
        in_clause_query = in_clause_query[:-1]
        in_clause_query += ")"
        return in_clause_query

    def _check_filter_columns(self, op):
        """
        Check if the columns present at operation are present at the
        query columns.
        """
        if not self._column_is_present(op['left_value']):
            logging.error("Column not present at query columns.")
            return False
        if op['right_value_type'] is Column:
            if not self._column_is_present(op['right_value']):
                logging.error("Column not present at query columns.")
                return False

    def _column_is_present(self, column_name):
        for column in self.columns:
            if column.name == column_name:
                return True
        return False
