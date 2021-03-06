from big_data_orm.resources.column import Column
from big_data_orm.resources.base_model import BaseModel


class AdwordsAdgroup(BaseModel):
    def __init__(self):
        self.__tablename__ = 'adgroup_report'
        self.id = Column(str, 'id')
        self.created_time = Column(int, 'created_time')
        self.account_id = Column(str, 'account_id')
        self.campaign_id = Column(str, 'campaign_id')
        self.adgroup_id = Column(str, 'adgroup_id')
        self.name = Column(str, 'name')
        self.state = Column(str, 'state')
        self.average_cost = Column(int, 'average_cost')
        self.average_cpc = Column(int, 'average_cpc')
        self.average_cpe = Column(int, 'average_cpe')
        self.average_cpm = Column(int, 'average_cpm')
        self.average_cpv = Column(int, 'average_cpv')
        self.all_conversion_rate = Column(str, 'all_conversion_rate')
        self.average_page_view = Column(float, 'average_page_view')
        self.average_position = Column(float, 'average_position')
        self.currency = Column(str, 'currency')
        self.account_name = Column(str, 'account_name')
        self.mobile_modifier = Column(str, 'mobile_modifier')
        self.network = Column(str, 'network')
        self.all_conversions = Column(float, 'all_conversions')
        self.average_time_on_site = Column(float, 'average_time_on_site')
        self.bounce_rate = Column(str, 'bounce_rate')
        self.campaign_name = Column(str, 'campaign_name')
        self.campaign_status = Column(str, 'campaign_status')
        self.campaign_assisted_conversions = Column(float, 'campaign_assisted_conversions')
        self.content_impression_share = Column(str, 'content_impression_share')
        self.conversions = Column(float, 'conversions')
        self.cost = Column(int, 'cost')
        self.cost_per_all_conversion = Column(int, 'cost_per_all_conversion')
        self.ctr = Column(str, 'ctr')
        self.impressions = Column(float, 'impressions')
