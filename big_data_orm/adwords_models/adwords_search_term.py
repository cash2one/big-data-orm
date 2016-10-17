from big_data_orm.resources.column import Column
from big_data_orm.resources.base_model import BaseModel


class AdwordsSearchTerm(BaseModel):
    def __init__(self):
        self.__tablename__ = 'adwords_search_terms_report'
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
