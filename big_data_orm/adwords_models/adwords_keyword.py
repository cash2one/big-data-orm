from big_data_orm.resources.column import Column
from big_data_orm.resources.base_model import BaseModel


class AdwordsKeyword(BaseModel):
    def __init__(self):
        self.__tablename__ = 'keyword_report'
        self.id = Column(str, 'id')
        self.created_time = Column(int, 'created_time')
        self.account_id = Column(str, 'account_id')
        self.keyword_id = Column(str, 'keyword_id')
        self.keyword = Column(str, 'keyword')
        self.active_view_cpm = Column(int, 'active_view_cpm')
        self.active_view_ctr = Column(str, 'active_view_ctr')
        self.active_view_impressions = Column(int, 'active_view_impressions')
        self.adgroup_id = Column(str, 'adgroup_id')
        self.all_conversion_rate = Column(str, 'all_conversion_rate')
        self.all_conversions = Column(float, 'all_conversions')
        self.average_cost = Column(int, 'average_cost')
        self.average_cpc = Column(int, 'average_cpc')
        self.average_cpe = Column(int, 'average_cpe')
        self.average_cpm = Column(int, 'average_cpm')
        self.average_cpv = Column(int, 'average_cpv')
        self.average_page_views = Column(float, 'average_page_views')
        self.average_position = Column(float, 'average_position')
        self.bounce_rate = Column(str, 'bounce_rate')
        self.click_conversion_rate = Column(str, 'click_conversion_rate')
        self.clicks = Column(int, 'clicks')
        self.conversion_rate = Column(str, 'conversion_rate')
        self.conversions = Column(float, 'conversions')
        self.converted_clicks = Column(int, 'converted_clicks')
        self.cost = Column(int, 'cost')
        self.cost_per_conversion = Column(int, 'cost_per_conversion')
        self.cost_per_converted_click = Column(int, 'cost_per_converted_click')
        self.campaign_id = Column(str, 'campaign_id')
        self.data_time = Column(int, 'data_time')
        self.all_conversion_value = Column(float, 'all_conversion_value')
        self.total_conversion_value = Column(float, 'total_conversion_value')
