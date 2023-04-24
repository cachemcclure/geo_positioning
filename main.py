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
import pyproj
import plotly.express as px


class dma:
    def __init__(self,
                 atts:dict):
        super(dma,self).__init__()
        self.raw_dict = atts
        self.dma_geo()
        self.gen_polygon()


    def dma_geo(self):
        if 'geometries' in self.raw_dict['geometry']:
            self.raw_rel_coordinates = self.raw_dict['geometry']['geometries']
            self.geo_type = 'MultiPolygon'
        else:
            self.raw_rel_coordinates = self.raw_dict['geometry']
            self.geo_type = 'Polygon'
        self.dma_name = self.raw_dict['properties']['NAME']
        self.dma_code = self.raw_dict['properties']['DMA']


    def gen_polygon(self):
        try:
            if self.geo_type == 'Polygon':
                self.polygon = Polygon([(float(xx[0]),float(xx[1])) for xx in self.raw_rel_coordinates['coordinates'][0] if (xx[0] is not None) and (xx[1] is not None)])
            elif self.geo_type == 'MultiPolygon':
                polygon_list = [Polygon([(float(xx[0]),float(xx[1])) for xx in yy['coordinates'][0] if (xx[0] is not None) and (xx[1] is not None)]) for yy in self.raw_rel_coordinates]
                self.polygon = MultiPolygon(polygon_list)
        except Exception as err:
            print(self.raw_rel_coordinates)
            

    def in_polygon(self,
                   coordinates:list):
        if len(coordinates) != 2:
            raise Exception('Supplied coordinates should be in lat-long pair')
        point = Point(float(coordinates[1]),float(coordinates[0]))
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


nielsen = jload(open('nielsen-definitions.json','r'))

if not exists('dma_definitions.csv'):
    ndf = pd.DataFrame(columns=['dma_id','rank','dma_name','tv_homes','percent_usa'])

    for xx in nielsen:
        temp = {'dma_id':xx,
                'rank':nielsen[xx]['Rank'],
                'dma_name':nielsen[xx]['Designated Market Area (DMA)'],
                'tv_homes':nielsen[xx]['TV Homes'],
                'percent_usa':nielsen[xx]['% of US']}
        ndf = ndf.append(temp,ignore_index=True)

    ndf.to_csv('dma_definitions.csv',index=False,header=True)
else:
    ndf = pd.read_csv('dma_definitions.csv')

coords = jload(open('full-nielsent-mkt-map.json','r'))

region_map = [dma(xx) for xx in coords['features']]

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

if not exists('dma_appended_data.csv'):
    user_data = pd.read_csv('mb_user_data.csv')
    temp_nm = []
    temp_id = []
    for index, row in user_data.iterrows():
        temp = find_dma(row['latitude'],row['longitude'],region_map)
        temp_nm += [temp[0]]
        temp_id += [temp[1]]
        if len(temp_nm)%70000 == 0:
            print(len(temp_nm))

    user_data['dma_name'] = temp_nm
    user_data['dma_id'] = temp_id

    user_data.to_csv('dma_appended_data.csv',index=False,header=True)
else:
    user_data = pd.read_csv('dma_appended_data.csv')

if not exists('dma_geodata.shp'):
    gdf = gpd.GeoDataFrame([[region_map[0].dma_name,region_map[0].dma_code]],geometry=gpd.GeoSeries(region_map[0].polygon))
    for xx in region_map[1:]:
        temp = gpd.GeoDataFrame([[xx.dma_name,xx.dma_code]],geometry=gpd.GeoSeries(xx.polygon))
        gdf = gdf.append(temp,ignore_index=True)
    #gdf.to_file('dma_geodata.shp')
else:
    gdf = gpd.read_file('dma_geodata.shp')

#gdf.plot(cmap='Blues')
#plt.show()
#print(gdf.columns)
gdf['id'] = gdf[1].astype(int)
gdf = gdf.merge(ndf.rename(columns={'dma_id':'id'}),on='id',how='left')

#gdf.to_crs(pyproj.CRS.from_epsg(4326),inplace=True)
fig = px.choropleth(gdf,
                     geojson=gdf.geometry,
                     locations=gdf.index,
                     hover_data=[0,1],
                     color='percent_usa',
                     title="DMA Regions",
                     labels={"0":"Region Name",
                             "1":"DMA Region",
                             "percent_usa":"Percent of Population"})
fig.update_geos(fitbounds="locations",
                visible=False)

fig.write_html('dma_regions.html',include_plotlyjs='cdn')
fig.show()
