from big_data_orm.resources.column import Column
from big_data_orm.resources.base_model import BaseModel


class AdwordsSearchTerm(BaseModel):
    def __init__(self):
        self.__tablename__ = 'search_terms_report'
        self.id = Column(str, 'id')
        self.adgroup_id = Column(str, 'adgroup_id')
        self.campaign_id = Column(str, 'campaign_id')
        self.account_id = Column(str, 'account_id')
        self.keyword_id = Column(str, 'keyword_id')
        self.search_term = Column(str, 'search_term')
        self.keyword = Column(str, 'keyword')
        self.clicks = Column(int, 'clicks')
        self.all_conversions = Column(float, 'all_conversions')
        self.conversions = Column(float, 'conversions')
        self.conversion_value = Column(float, 'conversion_value')
        self.impressions = Column(int, 'impressions')
        self.cost = Column(int, 'cost')
        self.average_position = Column(float, 'average_position')
        self.created_time = Column(int, 'created_time')
        self.data_time = Column(int, 'data_time')
        self.all_conversion_value = Column(float, 'all_conversion_value')
