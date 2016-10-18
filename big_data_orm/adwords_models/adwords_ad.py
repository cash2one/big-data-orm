from big_data_orm.resources.column import Column
from big_data_orm.resources.base_model import BaseModel


class AdwordsAd(BaseModel):
    def __init__(self):
        self.__tablename__ = 'adwords_ad_report'
        self.id = Column(str, 'id')
        self.ad_id = Column(str, 'ad_id')
        self.headline = Column(str, 'headline')
        self.cost_per_converted_click = Column(int, 'cost_per_converted_click')
        self.cost_per_conversion = Column(int, 'cost_per_conversion')
        self.conversions = Column(float, 'conversions')
        self.active_view_average_cpm = Column(int, 'active_view_average_cpm')
        self.active_view_average_ctr = Column(str, 'active_view_average_ctr')
        self.state = Column(str, 'state')
        self.ctr = Column(str, 'ctr')
        self.description = Column(str, 'description')
        self.display_url = Column(str, 'display_url')
        self.image_url = Column(str, 'image_url')
        self.account_id = Column(str, 'account_id')
        self.campaign_id = Column(str, 'campaign_id')
        self.adgroup_id = Column(str, 'adgroup_id')
        self.account_name = Column(str, 'account_name')
        self.created_time = Column(int, 'created_time')
