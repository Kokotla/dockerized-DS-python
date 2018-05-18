import gzip,zipfile,glob,math,os,argparse,time
import pandas as pd
import numpy as np
from multiprocessing import Pool,Process
from functools import partial
import sys
 
def dist(dirgd,entered_latitude,entered_longitude,radius):
	start_process=time.time()
	def haver_dist(latt,lonn,lat,lon):
		latt,lonn=np.nan_to_num(latt),np.nan_to_num(lonn)
		r=6371
		if latt==0 and lonn==0:
			return np.nan
		else:
			hav=math.sqrt(math.sin((latt-lat)/2)**2+math.cos(latt)*math.cos(lat)*math.sin((lonn-lon)/2)**2)
			d=2*r*np.arcsin(hav)
			return d
	current=Process()
	if dirgd.split(".")[-1]=="zip":
		open_csv=zipfile.ZipFile(dirgd)
		name=open_csv.namelist()
		csv_file=open_csv.open(name[0])
	elif dirgd.split(".")[-1]=="gz":
		csv_file=gzip.open(dirgd)

	data=pd.read_csv(csv_file,sep="\t",names=['GLOBALEVENTID','SQLDATE','MonthYear','Year','FractionDate','Actor1Code','Actor1Name','Actor1CountryCode','Actor1KnownGroupCode','Actor1EthnicCode','Actor1Religion1Code','Actor1Religion2Code','Actor1Type1Code','Actor1Type2Code','Actor1Type3Code','Actor2Code','Actor2Name','Actor2CountryCode','Actor2KnownGroupCode','Actor2EthnicCode','Actor2Religion1Code','Actor2Religion2Code','Actor2Type1Code','Actor2Type2Code','Actor2Type3Code','IsRootEvent','EventCode','EventBaseCode','EventRootCode','QuadClass','GoldsteinScale','NumMentions','NumSources','NumArticles','AvgTone','Actor1Geo_Type','Actor1Geo_FullName','Actor1Geo_CountryCode','Actor1Geo_ADM1Code','Actor1Geo_Lat','Actor1Geo_Long','Actor1Geo_FeatureID','Actor2Geo_Type','Actor2Geo_FullName','Actor2Geo_CountryCode','Actor2Geo_ADM1Code','Actor2Geo_Lat','Actor2Geo_Long','Actor2Geo_FeatureID','ActionGeo_Type','ActionGeo_FullName','ActionGeo_CountryCode','ActionGeo_ADM1Code','ActionGeo_Lat','ActionGeo_Long','ActionGeo_FeatureID','DATEADDED','SOURCEURL'
])
	dst=[]
	for i,j in zip(data['Actor2Geo_Lat'],data['Actor2Geo_Long']):
		dst.append(haver_dist(i,j,entered_latitude,entered_longitude))

	data=pd.concat([data,pd.DataFrame(dst)],axis=1)
	data=data[data[0]<=radius]
	t=data[['GLOBALEVENTID','GoldsteinScale','SOURCEURL']]
	if len(t)!=0:
		with open(os.path.join('./output',current.name+'_results.txt'),'w') as outfile:
			t.to_string(outfile,index=False)
			outfile.close()
	end_process=time.time()
	print('\n Process %s took %s \n'%(current.name,end_process-start_process))

def main():
	latitude=float(sys.argv[1])
	longitude=float(sys.argv[2])
	radius=float(sys.argv[3])

	
	p=Pool(processes=2)
	start=time.time()
	dist_in_km=partial(dist,entered_latitude=latitude,entered_longitude=longitude,radius=radius)
	results=p.map(dist_in_km,glob.glob('/data/*.export.*'))
	p.close()
	p.join()
	end=time.time()
	print("The whole process took",end-start)

	


if __name__ == '__main__':
	if os.path.isdir('./data') is True and os.path.isdir('./output') is True:
		main()
	else:
		print("Please create a folder 'data' and 'output' in the container and mout them to the local machine" )