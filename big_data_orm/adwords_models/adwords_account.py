from big_data_orm.resources.column import Column
from big_data_orm.resources.base_model import BaseModel


class AdwordsAccount(BaseModel):
    def __init__(self):
        self.__tablename__ = 'account_report'
        self.id = Column(str, 'id')
        self.created_time = Column(int, 'created_time')
        self.customer_id = Column(str, 'customer_id')
        self.account_id = Column(str, 'account_id')
        self.name = Column(str, 'name')
        self.clicks = Column(int, 'clicks')
        self.active_view_ctr = Column(str, 'active_view_ctr')
        self.active_view_impressions = Column(int, 'active_view_impressions')
        self.all_conversions = Column(float, 'all_conversions')
        self.average_cost = Column(int, 'average_cost')
        self.average_cpc = Column(int, 'average_cpc')
        self.average_cpe = Column(int, 'average_cpe')
        self.average_cpm = Column(int, 'average_cpm')
        self.average_cpv = Column(int, 'average_cpv')
        self.click_conversion_rate = Column(str, 'click_conversion_rate')
        self.currency = Column(str, 'currency')
        self.time_zone = Column(str, 'time_zone')
        self.active_view_viewable_impr = Column(str, 'active_view_viewable_impr')
        self.all_conversion_rate = Column(str, 'all_conversion_rate')
        self.all_conversion_value = Column(float, 'all_conversion_value')
        self.average_position = Column(float, 'average_position')
        self.conversion_rate = Column(str, 'conversion_rate')
        self.cost = Column(int, 'cost')
