from query_crawls.cric import Cric, CricMarketOption, CricLandOption

plate = {
    '鼓楼区': ['城北板块', '金山桥板块'],
    '经济开发区': ['城东板块', '高铁东城板块'],
}


def if_incomplete_annual(func):
    """装饰器：如果查询为不完整的年度数据，则分别查询完整年度和YTD数据，并进行拼合"""

    def wrapper(self, time, usage, **kwargs):
        # 需要拼合
        if time == 'annual':
            # 前几年
            time = self._year_range
            df = func(self, time, usage, **kwargs)
            df = df.drop('汇总') if '汇总' in df.index else df

            # 最后一年
            self.market_opt.rowtotal = True
            time = self._ytm
            df_ = func(self, time, usage, **kwargs)
            self.market_opt.rowtotal = False

            df = df.append(df_.loc['汇总'])
            df.index = self.annual_index

            return df

        # 无需拼合
        else:
            return func(self, time, usage, **kwargs)

    return wrapper


class Query(Cric):
    market_opt = CricMarketOption()
    land_opt = CricLandOption()
    year_range = '2009年01月:2018年04月'
    month_range = '2017年01月:2018年04月'
    # 以属性下供year_and_month方法拼合完整日期
    _year_range = '2009年:2017年'
    _ytm = '2018年01月:2018年04月'

    def __init__(self, province, city):
        super().__init__()
        self.market_opt.city = city
        self.land_opt.province = province
        self.land_opt.city = city

    @property
    def annual_index(self):
        # 前几年
        start, end = [int(x[2:4]) for x in self._year_range.split(':')]
        index = [f'{x:02d}' for x in range(start, end + 1)]

        # 最后一年
        year, month = self._ytm.split(':')[1][-6:].split('年')
        month = int(month.replace('月', ''))
        index += [f'{year}.1-{month}']

        return index

    @staticmethod
    def monthly_index_adjust(index):
        return [x[2:].replace('年', '.').replace('月', '') for x in index]

    @if_incomplete_annual
    def gxj(self, time, usage, region=None, district=None, rows='time'):
        """市场供销价"""
        self.market_opt.time = time
        self.market_opt.usage = usage
        self.market_opt.rows = rows
        self.market_opt.outputs = '供应面积,成交面积,成交均价'

        if region:
            self.market_opt.region_type = 'Region'
            self.market_opt.region = region
        if district:
            self.market_opt.region_type = 'District'
            self.market_opt.region = district

        return self.market(self.market_opt.data)

    @if_incomplete_annual
    def land_sold(self, time, usage, region='', district=''):
        """土地数据"""
        self.land_opt.by = 'time'
        self.land_opt.date = time
        self.land_opt.state = '成交'
        self.land_opt.usage = usage
        self.land_opt.region = region
        self.land_opt.district = district

        return self.land(self.land_opt.data)
