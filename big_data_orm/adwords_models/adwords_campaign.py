from big_data_orm.resources.column import Column
from big_data_orm.resources.base_model import BaseModel


class AdwordsCampaign(BaseModel):
    def __init__(self):
        self.__tablename__ = 'campaign_report'
        self.id = Column(str, 'id')
        self.created_time = Column(int, 'created_time')
        self.account_id = Column(str, 'account_id')
        self.campaign_id = Column(str, 'campaign_id')
        self.name = Column(str, 'name')
        self.clicks = Column(int, 'clicks')
        self.budget = Column(int, 'budget')
        self.average_cost = Column(int, 'average_cost')
        self.average_cpc = Column(int, 'average_cpc')
        self.average_cpe = Column(int, 'average_cpe')
        self.average_cpm = Column(int, 'average_cpm')
        self.average_cpv = Column(int, 'average_cpv')
        self.conversion_rate = Column(str, 'conversion_rate')
        self.campaign_state = Column(str, 'campaign_state')
        self.cost_per_conversion = Column(int, 'cost_per_conversion')
        self.account_currency_code = Column(str, 'account_currency_code')
        self.active_view_cpm = Column(int, 'active_view_cpm')
        self.active_view_ctr = Column(str, 'active_view_ctr')
        self.active_viewable_impressions = Column(float, 'active_viewable_impressions')
        self.network = Column(str, 'network')
        self.average_position = Column(float, 'average_position')
        self.bidding_strategy_name = Column(str, 'bidding_strategy_name')
        self.bounce_rate = Column(str, 'bounce_rate')
        self.mobile_bid_modifier = Column(str, 'mobile_bid_modifier')
        self.content_budget_lost_share = Column(str, 'content_budget_lost_share')
        self.conversions = Column(float, 'conversions')
        self.conversion_total_value = Column(float, 'conversion_total_value')
        self.cost = Column(int, 'cost')
        self.cost_per_all_conversion = Column(int, 'cost_per_all_conversion')
        self.impressions = Column(float, 'impressions')
        self.advertising_channel_type = Column(str, 'advertising_channel_type')
