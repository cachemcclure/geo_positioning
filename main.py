from sqlalchemy import create_engine
import pandas as pd
from pickle import load as pload
from pickle import dump as pdump
from os.path import exists
import urllib.request
from json import load as jload
from json import loads as jloads
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from shapely import MultiPolygon
import geopandas as gpd
import matplotlib.pyplot as plt


class dma:
    def __init__(self,
                 atts:dict):
        super(dma,self).__init__()
        self.raw_dict = atts
        self.dma_geo()
        self.gen_polygon()


    def dma_geo(self):
        self.raw_rel_coordinates = self.raw_dict['geometry']['coordinates'][0]
        self.geo_type = self.raw_dict['geometry']['type']
        self.ref_coordinates = [self.raw_dict['properties']['latitude'],self.raw_dict['properties']['longitude']]
        self.dma_name = self.raw_dict['properties']['dma_name']
        self.dma_code = self.raw_dict['properties']['dma_code']


    def gen_polygon(self):
        try:
            if self.geo_type == 'Polygon':
                self.polygon = Polygon([(float(self.ref_coordinates[0]) + float(xx[0]), float(self.ref_coordinates[1]) + float(xx[1])) \
                                        for xx in self.raw_rel_coordinates \
                                        if (xx[0] is not None) and \
                                        (xx[1] is not None)])
##                self.polygon = Polygon([(float(self.ref_coordinates[0]) + float(xx[0]), float(self.ref_coordinates[1]) + float(xx[1])) \
##                                        for xx in self.raw_rel_coordinates \
##                                        if (xx[0] is not None) and \
##                                        (xx[1] is not None) and \
##                                        (self.ref_coordinates[0] is not None) and \
##                                        (self.ref_coordinates[1] is not None)])
            elif self.geo_type == 'MultiPolygon':
                polygon_list = [Polygon([(float(self.ref_coordinates[0]) + float(xx[0]), float(self.ref_coordinates[1]) + float(xx[1])) \
                                         for xx in yy \
                                         if (xx[0] is not None) and \
                                         (xx[1] is not None)]) for yy in self.raw_rel_coordinates]
                self.polygon = MultiPolygon(polygon_list)
        except Exception as err:
            print(self.ref_coordinates)
            print(self.raw_rel_coordinates)
##            print(str(err)[:100])
            

    def in_polygon(self,
                   coordinates:list):
        if len(coordinates) != 2:
            raise Exception('Supplied coordinates should be in lat-long pair')
        point = Point(float(coordinates[0]),float(coordinates[1]))
        if self.polygon.contains(point):
            return [self.dma_name,self.dma_code]
        else:
            return False


    def show_polygon(self):
        self.gdf = gpd.GeoSeries(self.polygon)
        self.gdf.plot()
        plt.show()


def flatten(lst):
    if isinstance(lst,list):
        if isinstance(lst[0],list):
            for v in lst:
                yield from flatten(v)
        else:
            yield lst


## Retrieve Credentials if Exist
def ret_creds():
    """
    Args:
    none

    Returns:
    creds - dictionary of credentials needed for Redshift access
    """
    print('Retrieving credentials...')
    if exists('creds.pkl'):
        creds = pload(open('creds.pkl','rb'))
    else:
        raise Exception('No credentials found')
    print('Credentials retrieved.')
    return creds


## Create Redshift connection from credentials
def bld_cnxn(creds):
    """
    Args:
    creds(req) - dictionary of credentials needed for Redshift access

    Returns:
    client - Redshift connection engine
    """
    print('Building Redshift connection...')
    req_fields = ['redshift_username',
                  'redshift_password',
                  'redshift_host',
                  'redshift_port',
                  'redshift_database']
    for xx in req_fields:
        if xx not in creds:
            raise Exception(f'Missing required field: {xx}')
    rs_un = creds['redshift_username']
    rs_pw = creds['redshift_password']
    rs_host = creds['redshift_host']
    rs_port = creds['redshift_port']
    rs_db = creds['redshift_database']
    engine = create_engine('postgresql+psycopg2://'+rs_un+":"+rs_pw+"@"+rs_host+
                          ":"+rs_port+"/"+rs_db,encoding='latin-1',executemany_mode='batch',
                           executemany_batch_page_size=750)\
                          .execution_options(autocommit=True)
    print('Connected to database '+rs_db+' as '+rs_un)
    return engine


def find_dma(lat,
             long,
             dma_class_list):
    for dma_class in dma_class_list:
        out = dma_class.in_polygon([lat,long])
        if not isinstance(out,bool):
            return out
    return['NA','NA']


##dma_raw = pd.read_csv('geotargets-2023-03-28.csv')
##rn_cols = {}
##for col in dma_raw.columns:
##    rn_cols[col] = col.lower().replace(' ','_')
##dma_raw = dma_raw.rename(columns=rn_cols)

#creds = ret_creds()
##engine = bld_cnxn(creds)
##
##try:
##    dma_raw.to_sql('dma_definitions',schema='customer_appended_data',con=engine,
##                   if_exists='append',index=False,chunksize=1000,method='multi')
##except Exception as err:
##    print(str(err)[:250])

##try:
##    urllib.request.urlretrieve('https://raw.githubusercontent.com/simzou/nielsen-dma/master/nielsen-mkt-map.json',
##                               'nielsen-mkt-map.json')
##except Exception as err:
##    print(str(err)[:250])

coords = jload(open('nielsen-mkt-map.json','r'))

region_map = [dma(xx) for xx in coords if xx['properties']['latitude'] is not None]

sql = '''select distinct uu.id,
aa.latitude,
aa.longitude
from mb_production.orders oo
left join mb_production.users uu on uu.id = oo.user_id
left join mb_production.addresses aa on aa.id = oo.bill_address_id
where aa.latitude is not null
and aa.longitude is not null
and uu.id is not null;'''

##creds = ret_creds()
##engine = bld_cnxn(creds)
##
##try:
##    user_data = pd.read_sql(sql,con=engine)
##except Exception as err:
##    print(str(err)[:250])

user_data = pd.read_csv('mb_user_data.csv')
temp_nm = []
temp_id = []
for index, row in user_data.iterrows():
    temp = find_dma(row['latitude'],row['longitude'],region_map)
    temp_nm += [temp[0]]
    temp_id += [temp[1]]
    if len(temp_nm)%70000 == 0:
        print(len(temp_nm))
##    if temp[0] != 'NA':
##        print(f'Found DMA for {row["latitude"]}, {row["longitude"]}: {temp[0]} {temp[1]}')

user_data['dma_name'] = temp_nm
user_data['dma_id'] = temp_id
