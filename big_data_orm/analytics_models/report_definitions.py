from big_data_orm.resources.column import Column
from big_data_orm.resources.base_model import BaseModel


class ReportDefinitions(BaseModel):
    def __init__(self):
        self.__tablename__ = 'report_definitions'
        self.report_name = Column(str, 'report_name')
        self.table_name = Column(str, 'table_name')
        self.metrics = Column(
            dict, 'metrics',
            {
                'field_name': Column(str, 'field_name'),
                'type': Column(str, 'type')
            }
        )
        self.dimensions = Column(
            dict, 'dimensions',
            {
                'field_name': Column(str, 'field_name'),
                'type': Column(str, 'type')
            }
        )
